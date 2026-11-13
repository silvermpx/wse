"""The server's own answer to the clients' subscribe, unsubscribe and recovery
requests (client_subscriptions=True), and subscribe_with_recovery()."""

import asyncio
import json

import pytest

from tests.conftest import JWT_AUDIENCE, JWT_ISSUER, JWT_SECRET, make_token
from tests.test_delivery import EncryptedClient
from tests.test_integration import connect, drain_all, drain_until
from wse_server._wse_accel import RustWSEServer


def start_server(port: int, **options) -> RustWSEServer:
    options.setdefault("client_subscriptions", True)
    options.setdefault("recovery_enabled", True)
    srv = RustWSEServer(
        "127.0.0.1",
        port,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
        **options,
    )
    srv.enable_drain_mode()
    srv.start()
    return srv


def request(action: str, topics: list[str], recovery: dict | None = None) -> str:
    p = {"action": action, "topics": topics}
    if recovery is not None:
        p["recover"] = True
        p["recovery"] = recovery
    return json.dumps({"c": "WSE", "t": "subscription_update", "p": p, "v": 1})


async def messages_until_reply(ws) -> tuple[list[dict], dict]:
    """Topic messages, then the subscription reply that ends them."""
    got = []
    async with asyncio.timeout(5):
        while True:
            msg = json.loads(await ws.recv())
            if msg.get("t") == "subscription_update":
                return got, msg
            if "tp" in msg:
                got.append(msg)


def publish(srv: RustWSEServer, topic: str, n: int) -> None:
    srv.broadcast_local(topic, json.dumps({"t": "tick", "p": {"n": n}}))


class TestSubscribe:
    @pytest.mark.asyncio
    async def test_subscribe_reply_lists_the_topics(self, server_port):
        srv = start_server(server_port)
        try:
            async with await connect(server_port, make_token()) as ws:
                drain_until(srv, "auth_connect")
                await ws.send(request("subscribe", ["a", "b"]))
                _, reply = await messages_until_reply(ws)
                p = reply["p"]
                assert p["success"] is True
                assert sorted(p["success_topics"]) == ["a", "b"]
                assert p["failed_topics"] == []
                assert reply["id"]
                publish(srv, "a", 1)
                async with asyncio.timeout(5):
                    while True:
                        msg = json.loads(await ws.recv())
                        if msg.get("t") == "tick":
                            break
                assert msg["tp"] == "a"
                # The application does not see the request.
                assert not [e for e in drain_all(srv) if e[0] == "msg"]
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_topics_the_acl_denies_fail(self, server_port):
        srv = start_server(server_port)
        try:
            async with await connect(server_port, make_token()) as ws:
                _, conn_id, _ = drain_until(srv, "auth_connect")
                srv.set_topic_acl(conn_id, allow=["public:*"])
                await ws.send(request("subscribe", ["public:news", "admin:audit"]))
                _, reply = await messages_until_reply(ws)
                assert reply["p"]["success_topics"] == ["public:news"]
                assert reply["p"]["failed_topics"] == ["admin:audit"]
                assert reply["p"]["success"] is True
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_unsubscribe_stops_delivery_and_gets_no_reply(self, server_port):
        srv = start_server(server_port)
        try:
            async with await connect(server_port, make_token()) as ws:
                drain_until(srv, "auth_connect")
                await ws.send(request("subscribe", ["a"]))
                await messages_until_reply(ws)
                await ws.send(request("unsubscribe", ["a"]))
                await asyncio.sleep(0.2)
                publish(srv, "a", 1)
                await ws.send(json.dumps({"t": "ping", "p": {"timestamp": 1}}))
                async with asyncio.timeout(5):
                    while True:
                        msg = json.loads(await ws.recv())
                        assert msg.get("t") not in ("tick", "subscription_update"), msg
                        if msg.get("t") == "PONG":
                            break
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_requests_reach_the_application_when_off(self, server_port):
        srv = start_server(server_port, client_subscriptions=False)
        try:
            async with await connect(server_port, make_token()) as ws:
                drain_until(srv, "auth_connect")
                await ws.send(request("subscribe", ["a"]))
                event = drain_until(srv, "msg")
                assert event[2]["t"] == "subscription_update"
        finally:
            srv.stop()


class TestRecovery:
    @pytest.mark.asyncio
    async def test_each_topic_recovers_from_its_own_position(self, server_port):
        srv = start_server(server_port)
        try:
            positions = {}
            async with await connect(server_port, make_token()) as ws:
                drain_until(srv, "auth_connect")
                await ws.send(request("subscribe", ["a", "b"]))
                await messages_until_reply(ws)
                for n in range(3):
                    publish(srv, "a", n)
                    publish(srv, "b", n)
                async with asyncio.timeout(5):
                    while len(positions) < 2 or any(o < 2 for _, o in positions.values()):
                        msg = json.loads(await ws.recv())
                        if "tp" in msg:
                            positions[msg["tp"]] = (msg["e"], msg["o"])
            # Missed while away: a gets 2 more, b gets 4 more.
            for n in range(3, 5):
                publish(srv, "a", n)
            for n in range(3, 7):
                publish(srv, "b", n)
            async with await connect(server_port, make_token()) as ws:
                drain_until(srv, "auth_connect")
                recovery = {t: {"epoch": e, "offset": o} for t, (e, o) in positions.items()}
                await ws.send(request("subscribe", ["a", "b"], recovery))
                replayed, reply = await messages_until_reply(ws)
            by_topic = {}
            for msg in replayed:
                by_topic.setdefault(msg["tp"], []).append(msg["p"]["n"])
            assert by_topic == {"a": [3, 4], "b": [3, 4, 5, 6]}
            rec = reply["p"]["recovery"]
            assert rec["a"]["recovered"] is True and rec["a"]["count"] == 2
            assert rec["b"]["recovered"] is True and rec["b"]["count"] == 4
            assert rec["a"]["offset"] == 4 and rec["b"]["offset"] == 6
            assert rec["a"]["epoch"] == positions["a"][0]
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_an_unknown_epoch_reports_the_last_offset_before_subscribing(self, server_port):
        srv = start_server(server_port)
        try:
            for n in range(5):
                publish(srv, "a", n)
            async with await connect(server_port, make_token()) as ws:
                drain_until(srv, "auth_connect")
                await ws.send(
                    request("subscribe", ["a"], {"a": {"epoch": "00000000", "offset": 1}})
                )
                replayed, reply = await messages_until_reply(ws)
            assert replayed == []
            rec = reply["p"]["recovery"]["a"]
            assert rec["recovered"] is False
            # Offsets 0..4 were published before the subscription; 5 on comes live.
            assert rec["offset"] == 4
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_encrypted_requests_get_encrypted_answers(self, server_port):
        srv = start_server(server_port)
        try:
            client = await EncryptedClient.open(server_port)
            drain_until(srv, "auth_connect")
            await client.send(
                {
                    "c": "WSE",
                    "t": "subscription_update",
                    "p": {"action": "subscribe", "topics": ["a"]},
                    "v": 1,
                }
            )
            async with asyncio.timeout(5):
                while True:
                    msg, encrypted = await client.recv()
                    if msg.get("t") == "subscription_update":
                        break
            assert encrypted
            assert msg["p"]["success_topics"] == ["a"]
            await client.ws.close()
        finally:
            srv.stop()


class TestSubscribeWithRecovery:
    @pytest.mark.asyncio
    async def test_positions_per_topic_and_the_topics_taken(self, server_port):
        srv = start_server(
            server_port, client_subscriptions=False, max_subscriptions_per_connection=2
        )
        try:
            for n in range(4):
                publish(srv, "a", n)
                publish(srv, "b", n)
            async with await connect(server_port, make_token()):
                _, conn_id, _ = drain_until(srv, "auth_connect")
                a_epoch = srv.subscribe_with_recovery(conn_id, ["a"])["topics"]["a"]["epoch"]
                srv.unsubscribe_connection(conn_id, ["a"])
                b_epoch = srv.subscribe_with_recovery(conn_id, ["b"])["topics"]["b"]["epoch"]
                srv.unsubscribe_connection(conn_id, ["b"])
                result = srv.subscribe_with_recovery(
                    conn_id,
                    ["a", "b", "c"],
                    recover=True,
                    positions={
                        "a": {"epoch": a_epoch, "offset": 1},
                        "b": {"epoch": b_epoch, "offset": 2},
                    },
                )
                topics = result["topics"]
                assert topics["a"]["recovered"] and topics["a"]["count"] == 2
                assert topics["b"]["recovered"] and topics["b"]["count"] == 1
                assert topics["c"]["subscribed"] is False
                assert result["recovered"] is False  # c was not taken
                with pytest.raises(ValueError):
                    srv.subscribe_with_recovery(
                        conn_id, ["a"], recover=True, positions={"a": {"epoch": "zz", "offset": 1}}
                    )
        finally:
            srv.stop()
