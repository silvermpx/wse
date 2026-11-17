"""The Python client against the real server: subscriptions, recovery after a
reconnect, and both E2E designs."""

import asyncio
import importlib.util
import json

import pytest

from tests.conftest import JWT_AUDIENCE, JWT_ISSUER, JWT_SECRET, make_token
from tests.test_delivery import identity_key
from wse_server._wse_accel import RustWSEServer

wse_client = pytest.importorskip("wse_client")
needs_crypto = pytest.mark.skipif(
    importlib.util.find_spec("cryptography") is None, reason="cryptography not installed"
)


def start_server(port: int, **options) -> RustWSEServer:
    options.setdefault("client_subscriptions", True)
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


def client_for(port: int, **options):
    return wse_client.AsyncWSEClient(
        f"ws://127.0.0.1:{port}/wse",
        token=make_token("pyclient"),
        reconnect=wse_client.ReconnectConfig(base_delay=0.05, max_delay=0.2, jitter=False),
        **options,
    )


async def next_of_type(client, kind: str, timeout: float = 5.0):
    async with asyncio.timeout(timeout):
        while True:
            event = await client.recv()
            if event.type == kind:
                return event


async def subscribed(srv: RustWSEServer, topic: str) -> None:
    async with asyncio.timeout(5):
        while srv.get_topic_subscriber_count(topic) == 0:
            await asyncio.sleep(0.02)


async def inbound_types(srv: RustWSEServer) -> list[str]:
    """The types of the messages clients sent, once some came."""
    async with asyncio.timeout(5):
        while True:
            got = [e[2]["t"] for e in srv.drain_inbound(64, 0) if e[0] == "msg"]
            if got:
                # What comes with them in the next moment, too.
                await asyncio.sleep(0.2)
                got += [e[2]["t"] for e in srv.drain_inbound(64, 0) if e[0] == "msg"]
                return got
            # The client's own tasks run meanwhile.
            await asyncio.sleep(0.02)


def publish(srv: RustWSEServer, topic: str, n: int) -> None:
    srv.broadcast_local(topic, json.dumps({"t": "tick", "p": {"n": n}}))


class TestPlain:
    @pytest.mark.asyncio
    async def test_the_client_subscribes_and_receives(self, server_port):
        srv = start_server(server_port)
        try:
            client = client_for(server_port, topics=["news"])
            await client.connect()
            await subscribed(srv, "news")
            publish(srv, "news", 1)
            assert (await next_of_type(client, "tick")).payload == {"n": 1}
            await client.send("chat", {"text": "hi"})
            async with asyncio.timeout(5):
                while True:
                    events = [e for e in srv.drain_inbound(64, 50) if e[0] == "msg"]
                    if events:
                        break
            assert events[0][2]["p"] == {"text": "hi"}
            await client.disconnect()
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_missed_messages_come_back_after_a_reconnect(self, server_port):
        srv = start_server(server_port, recovery_enabled=True)
        try:
            client = client_for(server_port, topics=["prices"])
            await client.connect()
            await subscribed(srv, "prices")
            for n in range(3):
                publish(srv, "prices", n)
            for n in range(3):
                assert (await next_of_type(client, "tick")).payload == {"n": n}
            # Drop every connection; publish while the client is away.
            for conn_id in srv.get_connections():
                srv.disconnect(conn_id)
            async with asyncio.timeout(5):
                while srv.get_topic_subscriber_count("prices"):
                    await asyncio.sleep(0.02)
            for n in range(3, 6):
                publish(srv, "prices", n)
            got = [(await next_of_type(client, "tick", timeout=10)).payload["n"] for _ in range(3)]
            assert got == [3, 4, 5]
            await client.disconnect()
        finally:
            srv.stop()


class TestRecoverOrResync:
    @pytest.mark.asyncio
    async def test_a_reconnect_recovers_from_what_was_committed(self, server_port):
        """The delivery against the real server: the topic is held until the
        application resynced it; after a reconnect, recovery starts from what
        it committed, so what came but was not applied comes again."""
        srv = start_server(server_port, recovery_enabled=True)
        try:
            client = client_for(
                server_port,
                topics=["prices"],
                delivery=wse_client.DeliveryConfig(consumer="board"),
            )
            await client.connect()
            resync = await next_of_type(client, "resync")
            assert resync.payload["reasons"] == {"prices": "cold_start"}
            # A send that requires the topic waits for the resync; one that
            # requires nothing goes past it.
            queued = await client.send("cancel", {"id": 1}, requires=["prices"])
            assert queued is wse_client.SendResult.QUEUED
            assert await client.send("note", {}) is wse_client.SendResult.SENT
            assert await inbound_types(srv) == ["note"]
            await subscribed(srv, "prices")
            for n in range(3):
                publish(srv, "prices", n)
            with pytest.raises(TimeoutError):
                await next_of_type(client, "tick", timeout=0.3)
            client.resynced(resync.payload["round"])
            assert await inbound_types(srv) == ["cancel"]
            ticks = [await next_of_type(client, "tick") for _ in range(3)]
            assert [t.payload["n"] for t in ticks] == [0, 1, 2]
            # 0 and 1 applied; 2 came but was not.
            client.commit(ticks[0])
            client.commit(ticks[1])
            for conn_id in srv.get_connections():
                srv.disconnect(conn_id)
            again = await next_of_type(client, "resync", timeout=10)
            assert again.payload["reasons"] == {"prices": "reconnected"}
            client.resynced(again.payload["round"])
            assert (await next_of_type(client, "tick", timeout=10)).payload == {"n": 2}
            await client.disconnect()
        finally:
            srv.stop()


async def taken_messages(srv: RustWSEServer, count: int) -> list[dict]:
    """The next *count* messages clients sent, as the application took them."""
    out: list[dict] = []
    async with asyncio.timeout(5):
        while len(out) < count:
            out += [e[2] for e in srv.drain_inbound(64, 0) if e[0] == "msg"]
            if len(out) < count:
                await asyncio.sleep(0.02)
    return out


class TestOutbox:
    @pytest.mark.asyncio
    async def test_a_message_waits_in_the_outbox_until_the_server_confirms_it(
        self, server_port
    ):
        """Numbered, said delivered by its id; sent with no connection, it
        goes on the next."""
        srv = start_server(server_port)
        try:
            client = client_for(server_port)
            delivered: list[str] = []
            client.on("delivered")(lambda e: delivered.extend(e.payload["ids"]))
            await client.connect()
            await client.send("order", {"n": 1}, id="mine-1")
            (taken,) = await taken_messages(srv, 1)
            assert isinstance(taken["s"], int) and taken["id"] == "mine-1"
            async with asyncio.timeout(5):
                while "mine-1" not in delivered:
                    await asyncio.sleep(0.02)
            assert client.kept == 0

            # The connection goes: what is sent meanwhile waits, and goes on
            # the next one, numbered on.
            for conn_id in srv.get_connections():
                srv.disconnect(conn_id)
            async with asyncio.timeout(5):
                while client.is_connected:
                    await asyncio.sleep(0.01)
            await client.send("order", {"n": 2}, id="mine-2")
            (again,) = await taken_messages(srv, 1)
            assert (again["id"], again["s"]) == ("mine-2", taken["s"] + 1)
            async with asyncio.timeout(5):
                while "mine-2" not in delivered:
                    await asyncio.sleep(0.02)
            await client.disconnect()
        finally:
            srv.stop()


class TestMsgpack:
    @pytest.mark.asyncio
    async def test_msgpack_both_ways(self, server_port):
        pytest.importorskip("msgpack")
        srv = start_server(server_port)
        try:
            client = client_for(server_port, topics=["news"], wire_format="msgpack")
            await client.connect()
            await subscribed(srv, "news")
            publish(srv, "news", 3)
            assert (await next_of_type(client, "tick")).payload == {"n": 3}
            await client.send("chat", {"text": "packed"})
            async with asyncio.timeout(5):
                while True:
                    events = [e for e in srv.drain_inbound(64, 50) if e[0] == "msg"]
                    if events:
                        break
            assert events[0][2]["p"] == {"text": "packed"}
            await client.disconnect()
        finally:
            srv.stop()


@needs_crypto
class TestEncryption:
    @pytest.mark.asyncio
    async def test_a_pinned_session(self, server_port):
        srv = start_server(server_port, e2e_identity_key=identity_key())
        try:
            client = client_for(
                server_port,
                topics=["news"],
                encryption=True,
                e2e_server_keys=[srv.e2e_identity_public_key()],
            )
            await client.connect()
            await subscribed(srv, "news")
            publish(srv, "news", 7)
            assert (await next_of_type(client, "tick")).payload == {"n": 7}
            await client.send("chat", {"text": "sealed"})
            async with asyncio.timeout(5):
                while True:
                    events = [e for e in srv.drain_inbound(64, 50) if e[0] == "msg"]
                    if events:
                        break
            assert events[0][2]["p"] == {"text": "sealed"}
            # Heartbeats in the session are sealed too: still connected.
            await asyncio.sleep(0.3)
            assert client.is_connected
            await client.disconnect()
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_a_wrong_pin_never_falls_back_to_plaintext(self, server_port):
        srv = start_server(server_port, e2e_identity_key=identity_key())
        other = RustWSEServer("127.0.0.1", 0, e2e_identity_key=identity_key())
        try:
            client = client_for(
                server_port,
                encryption=True,
                e2e_server_keys=[other.e2e_identity_public_key()],
            )
            with pytest.raises(wse_client.WSEEncryptionError):
                await client.connect()
            await client.disconnect()
        finally:
            srv.stop()

    @pytest.mark.asyncio
    async def test_the_first_design_still_works(self, server_port):
        srv = start_server(server_port)
        try:
            client = client_for(server_port, topics=["news"], encryption=True, e2e_legacy=True)
            await client.connect()
            await subscribed(srv, "news")
            publish(srv, "news", 9)
            assert (await next_of_type(client, "tick")).payload == {"n": 9}
            await client.disconnect()
        finally:
            srv.stop()


class TestSessionHealth:
    @pytest.mark.asyncio
    @needs_crypto
    async def test_sealed_server_pings_are_answered(self, server_port):
        # Idle for 3 s without a frame from the client closes it; the
        # client's own heartbeat comes every 15 s, so only its answers to the
        # server's sealed pings keep it open.
        srv = start_server(
            server_port, e2e_identity_key=identity_key(), ping_interval=1, idle_timeout=3
        )
        try:
            client = client_for(
                server_port, encryption=True, e2e_server_keys=[srv.e2e_identity_public_key()]
            )
            lost = []
            client.on("disconnected")(lost.append)
            await client.connect()
            await asyncio.sleep(4.5)
            assert client.is_connected and lost == []
            await client.disconnect()
        finally:
            srv.stop()

    @pytest.mark.asyncio
    @needs_crypto
    async def test_a_session_the_server_never_confirms_sends_nothing(
        self, server_port, monkeypatch
    ):
        import wse_client.client as client_mod

        monkeypatch.setattr(client_mod, "SERVER_READY_TIMEOUT", 1.0)
        # No jwt_secret and no accept_connection(): the session opens, the
        # sealed server_ready never comes.
        srv = RustWSEServer("127.0.0.1", server_port, e2e_identity_key=identity_key())
        srv.enable_drain_mode()
        srv.start()
        try:
            client = wse_client.AsyncWSEClient(
                f"ws://127.0.0.1:{server_port}/wse",
                topics=["news"],
                encryption=True,
                e2e_server_keys=[srv.e2e_identity_public_key()],
                reconnect=None,
            )
            with pytest.raises(wse_client.WSEEncryptionError, match="server_ready"):
                await client.connect()
            await asyncio.sleep(0.2)
            events = srv.drain_inbound(64, 50)
            assert not [e for e in events if e[0] == "msg"]
            await client.disconnect()
        finally:
            srv.stop()


class TestApplicationAccepts:
    """A server without jwt_secret: the application accepts each connection."""

    @staticmethod
    def accept_later(srv: RustWSEServer, delay: float) -> asyncio.Task:
        async def accept() -> None:
            async with asyncio.timeout(5):
                while True:
                    events = await asyncio.to_thread(srv.drain_inbound, 64, 50)
                    for kind, conn_id, _ in events:
                        if kind == "connect":
                            await asyncio.sleep(delay)
                            srv.accept_connection(conn_id, "bob")
                            return

        return asyncio.ensure_future(accept())

    @pytest.mark.asyncio
    @pytest.mark.parametrize("encryption", [False, pytest.param(True, marks=needs_crypto)])
    async def test_topics_wait_for_the_acceptance(self, server_port, encryption):
        options = {"e2e_identity_key": identity_key()} if encryption else {}
        srv = RustWSEServer("127.0.0.1", server_port, client_subscriptions=True, **options)
        srv.enable_drain_mode()
        srv.start()
        try:
            accepting = self.accept_later(srv, 0.3)
            client_options = (
                {"encryption": True, "e2e_server_keys": [srv.e2e_identity_public_key()]}
                if encryption
                else {}
            )
            client = wse_client.AsyncWSEClient(
                f"ws://127.0.0.1:{server_port}/wse", topics=["news"], **client_options
            )
            await client.connect()
            await accepting
            await subscribed(srv, "news")
            publish(srv, "news", 3)
            assert (await next_of_type(client, "tick")).payload == {"n": 3}
            await client.disconnect()
        finally:
            srv.stop()
