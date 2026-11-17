"""Two-node clusters on loopback: forming, peers leaving, the cluster's secret,
mutual TLS.

Instance ids are time-ordered and made in connect_cluster(), so the node that
joins first holds the lower id. Each scenario names which node dials.
"""

import asyncio
import base64
import datetime
import ipaddress
import json
import time

import pytest

from tests.conftest import _next_port
from wse_server import RustWSEServer

# Every link is TLS: from this secret, unless a test gives certificates.
SECRET = "a test cluster secret of at least 32 bytes"


def wait_for(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.1)
    return predicate()


class Node:
    def __init__(self, recovery=False, single_port=False, **options):
        self.port = _next_port()
        self.server = RustWSEServer(
            "127.0.0.1", self.port, max_connections=10, recovery_enabled=recovery, **options
        )
        self.server.enable_drain_mode()
        self.server.start()
        self.single_port = single_port
        self.cluster_port = None if single_port else _next_port()
        self.running = True

    @property
    def addr(self):
        return f"127.0.0.1:{self.port if self.single_port else self.cluster_port}"

    @property
    def peers(self):
        return self.server.cluster_peers_count()

    def join(self, *peers, **security):
        listen = {"single_port": True} if self.single_port else {"cluster_port": self.cluster_port}
        security = security or {"cluster_secret": SECRET}
        self.server.connect_cluster(peers=[p.addr for p in peers], **listen, **security)

    def stop(self):
        if self.running:
            self.running = False
            self.server.stop()


@pytest.fixture()
def nodes():
    made = []

    def make(**options):
        node = Node(**options)
        made.append(node)
        return node

    yield make
    for node in made:
        node.stop()


def test_nodes_dialing_each_other_keep_one_link(nodes):
    a, b = nodes(), nodes()
    a.join(b)
    b.join(a)
    assert wait_for(lambda: a.peers == 1 and b.peers == 1)
    time.sleep(1.0)  # the duplicate link is dropped, the kept one stays
    assert (a.peers, b.peers) == (1, 1)


def test_a_node_known_under_two_addresses_keeps_one_steady_link(nodes):
    a, b = nodes(), nodes()
    b.join()
    # Two spellings of one node: two dialers on a.
    a.server.connect_cluster(
        peers=[b.addr, f"localhost:{b.cluster_port}"],
        cluster_port=a.cluster_port,
        cluster_secret=SECRET,
    )
    assert wait_for(lambda: a.peers == 1 and b.peers == 1)
    samples = []
    deadline = time.monotonic() + 4.0
    while time.monotonic() < deadline:
        samples.append((a.peers, b.peers))
        time.sleep(0.1)
    assert set(samples) == {(1, 1)}, samples


def test_a_node_listed_among_its_own_peers_stops_dialing_itself(nodes):
    # The same list on every node, its own address included.
    a = nodes()
    a.server.connect_cluster(peers=[a.addr], cluster_port=a.cluster_port, cluster_secret=SECRET)
    time.sleep(1.0)
    first = a.server.health_snapshot().get("cluster_reconnect_count", 0)
    time.sleep(4.0)
    assert a.server.health_snapshot().get("cluster_reconnect_count", 0) == first
    assert a.peers == 0


def test_newer_node_joins_through_an_older_seed(nodes):
    seed, newcomer = nodes(), nodes()
    seed.join()
    newcomer.join(seed)
    assert wait_for(lambda: seed.peers == 1 and newcomer.peers == 1)


def test_receiver_forgets_a_dialer_that_left(nodes):
    dialer, receiver = nodes(), nodes()
    dialer.join(receiver)
    receiver.join(dialer)
    assert wait_for(lambda: dialer.peers == 1 and receiver.peers == 1)
    dialer.stop()
    assert wait_for(lambda: receiver.peers == 0)
    assert receiver.server.cluster_info() == []


def test_dialer_forgets_a_receiver_that_left(nodes):
    dialer, receiver = nodes(), nodes()
    dialer.join(receiver)
    receiver.join(dialer)
    assert wait_for(lambda: dialer.peers == 1 and receiver.peers == 1)
    receiver.stop()
    assert wait_for(lambda: dialer.peers == 0)


def test_restarted_peer_is_connected_again(nodes):
    a, b = nodes(), nodes()
    a.join(b)
    b.join(a)
    assert wait_for(lambda: a.peers == 1 and b.peers == 1)
    port = b.cluster_port
    b.stop()
    assert wait_for(lambda: a.peers == 0)
    c = nodes()
    c.cluster_port = port  # same address, new instance
    c.join(a)
    assert wait_for(lambda: a.peers == 1 and c.peers == 1, timeout=20.0)


async def subscribe_next_client(node, topics, queue_group=None):
    """Wait for the next client's connect event on `node` and subscribe it."""
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        for event in node.server.drain_inbound(16, 50):
            if event[0] == "connect":
                node.server.subscribe_connection(event[1], topics, queue_group=queue_group)
                return
        await asyncio.sleep(0.05)
    raise AssertionError("no client connected")


async def messages_of_type(ws, kind, count):
    found = []
    while len(found) < count:
        frame = await asyncio.wait_for(ws.recv(), timeout=5)
        text = frame if isinstance(frame, str) else frame.decode(errors="replace")
        if f'"{kind}"' in text:
            found.append(json.loads(text[text.index("{") :]))
    return found


def test_relayed_messages_carry_the_position_they_got_at_their_origin(nodes):
    websockets = pytest.importorskip("websockets")
    origin, relay = nodes(recovery=True), nodes(recovery=True)
    origin.join(relay)
    relay.join(origin)
    assert wait_for(lambda: origin.peers == 1 and relay.peers == 1)

    async def scenario():
        async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as ws:
            await subscribe_next_client(relay, ["news"])
            await asyncio.sleep(0.5)  # the SUB reaches the origin
            for n in range(5):
                origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": n}}))
            return await messages_of_type(ws, "news", 5)

    messages = asyncio.run(scenario())
    assert [m["p"]["n"] for m in messages] == list(range(5))
    assert all(m["tp"] == "news" for m in messages)
    assert len({m["e"] for m in messages}) == 1
    offsets = [m["o"] for m in messages]
    assert offsets == list(range(offsets[0], offsets[0] + 5)), offsets


class Blink:
    """A TCP proxy in front of a node's cluster port that can drop every link
    through it and refuse new ones for a while."""

    def __init__(self, target):
        self.target = target
        self.port = _next_port()
        self.open = True
        self.links = set()

    @property
    def addr(self):
        return f"127.0.0.1:{self.port}"

    async def start(self):
        self.server = await asyncio.start_server(self._link, "127.0.0.1", self.port)

    async def _link(self, down_reader, down_writer):
        if not self.open:
            down_writer.transport.abort()
            return
        up_reader, up_writer = await asyncio.open_connection("127.0.0.1", self.target.cluster_port)
        pair = (down_writer, up_writer)
        self.links.add(pair)

        async def pipe(src, dst):
            try:
                while data := await src.read(65536):
                    dst.write(data)
                    await dst.drain()
            except ConnectionError:
                pass
            finally:
                dst.transport.abort()

        await asyncio.gather(pipe(down_reader, up_writer), pipe(up_reader, down_writer))
        self.links.discard(pair)

    def cut(self):
        self.open = False
        for down, up in list(self.links):
            down.transport.abort()
            up.transport.abort()

    def close(self):
        self.cut()
        self.server.close()


async def until(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "timed out"
        await asyncio.sleep(0.05)


def test_messages_published_while_a_link_was_down_arrive_after_it(nodes):
    """A node whose link blinked asks the other, once it is back, for what
    it missed: its clients get it in order, then what is published next."""
    websockets = pytest.importorskip("websockets")
    origin, relay = nodes(recovery=True), nodes(recovery=True)
    origin.join()

    def publish(n):
        origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": n}}))

    async def scenario():
        blink = Blink(origin)
        await blink.start()
        try:
            relay.join(blink)
            await until(lambda: origin.peers == 1 and relay.peers == 1)
            async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as ws:
                await subscribe_next_client(relay, ["news"])
                await asyncio.sleep(0.5)  # the SUB reaches the origin
                publish(0)
                publish(1)
                seen = await messages_of_type(ws, "news", 2)

                blink.cut()
                await until(lambda: origin.peers == 0 and relay.peers == 0)
                publish(2)
                publish(3)
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(ws.recv(), timeout=0.5)

                blink.open = True
                await until(lambda: origin.peers == 1 and relay.peers == 1, timeout=20.0)
                publish(4)
                seen += await messages_of_type(ws, "news", 3)
        finally:
            blink.close()
        return seen

    seen = asyncio.run(scenario())
    assert [m["p"]["n"] for m in seen] == [0, 1, 2, 3, 4]
    offsets = [m["o"] for m in seen]
    assert offsets == list(range(offsets[0], offsets[0] + 5)), offsets
    assert len({m["e"] for m in seen}) == 1


async def next_connect(node):
    """The connection id of the next client that connects to `node`."""
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        for event in node.server.drain_inbound(16, 50):
            if event[0] == "connect":
                return event[1]
        await asyncio.sleep(0.05)
    raise AssertionError("no client connected")


def test_a_stream_begun_while_a_link_was_down_arrives_after_it(nodes):
    """A node publishes to a topic for the first time while its link to the
    other is down: the other node's clients get it once the link is back, and
    a client that comes back from before it recovers it there."""
    websockets = pytest.importorskip("websockets")
    origin, relay = nodes(recovery=True), nodes(recovery=True)
    origin.join()

    def publish(node, n):
        node.server.broadcast("news", json.dumps({"t": "news", "p": {"n": n}}))

    async def scenario():
        blink = Blink(origin)
        await blink.start()
        try:
            relay.join(blink)
            await until(lambda: origin.peers == 1 and relay.peers == 1)
            async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as live:
                await subscribe_next_client(relay, ["news"])
                await asyncio.sleep(0.5)  # the SUB reaches the origin
                # The relay's own message: where a client stands before the gap.
                publish(relay, 0)
                (before,) = await messages_of_type(live, "news", 1)

                blink.cut()
                await until(lambda: origin.peers == 0 and relay.peers == 0)
                # The origin's first messages of the topic.
                publish(origin, 1)
                publish(origin, 2)

                blink.open = True
                await until(lambda: origin.peers == 1 and relay.peers == 1, timeout=20.0)
                seen = await messages_of_type(live, "news", 2)

            async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as back:
                conn_id = await next_connect(relay)
                result = relay.server.subscribe_with_recovery(
                    conn_id,
                    ["news"],
                    recover=True,
                    positions={"news": {"epoch": before["e"], "offset": before["o"]}},
                )
                replayed = await messages_of_type(back, "news", 2)
        finally:
            blink.close()
        return seen, result, replayed

    seen, result, replayed = asyncio.run(scenario())
    assert [m["p"]["n"] for m in seen] == [1, 2]
    assert [m["o"] for m in seen] == [0, 1], "the stream from its first message"
    assert result["topics"]["news"]["recovered"] is True
    assert [m["p"]["n"] for m in replayed] == [1, 2]


def test_a_signed_message_crosses_the_cluster_unchanged(nodes):
    websockets = pytest.importorskip("websockets")
    pytest.importorskip("cryptography")
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature

    key = ec.generate_private_key(ec.SECP256R1()).private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    origin, relay = nodes(recovery=True, signing_key=key), nodes(recovery=True)
    origin.join(relay)
    relay.join(origin)
    assert wait_for(lambda: origin.peers == 1 and relay.peers == 1)

    async def scenario():
        async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as ws:
            await subscribe_next_client(relay, ["orders"])
            await asyncio.sleep(0.5)  # the SUB reaches the origin
            origin.server.broadcast("orders", origin.server.sign("fill", {"qty": "2"}, "orders"))
            return (await messages_of_type(ws, "signed", 1))[0]

    envelope = asyncio.run(scenario())
    # Stamped by the origin around the signed text, which arrives as signed.
    assert envelope["tp"] == "orders" and "o" in envelope
    public = base64.b64decode(origin.server.signing_public_key())
    raw = base64.b64decode(envelope["sig"])
    der = encode_dss_signature(int.from_bytes(raw[:32], "big"), int.from_bytes(raw[32:], "big"))
    ec.EllipticCurvePublicKey.from_encoded_point(ec.SECP256R1(), public).verify(
        der, b"WSE-SIG server\0" + envelope["sb"].encode(), ec.ECDSA(hashes.SHA256())
    )
    assert json.loads(envelope["sb"])["p"] == {"qty": "2"}


def test_a_queue_group_on_another_node_gets_the_topic(nodes):
    websockets = pytest.importorskip("websockets")
    origin, worker = nodes(recovery=True), nodes(recovery=True)
    origin.join(worker)
    worker.join(origin)
    assert wait_for(lambda: origin.peers == 1 and worker.peers == 1)

    async def scenario():
        async with websockets.connect(f"ws://127.0.0.1:{worker.port}/wse") as ws:
            await subscribe_next_client(worker, ["tasks"], queue_group="workers")
            await asyncio.sleep(0.5)  # the membership's SUB reaches the origin
            for n in range(3):
                origin.server.broadcast("tasks", json.dumps({"t": "task", "p": {"n": n}}))
            return await messages_of_type(ws, "task", 3)

    tasks = asyncio.run(scenario())
    assert [t["p"]["n"] for t in tasks] == [0, 1, 2]
    # A member sees only some offsets of a topic: no recovery stamp.
    assert all("o" not in t for t in tasks)


def test_subscriptions_made_before_joining_reach_peers(nodes):
    websockets = pytest.importorskip("websockets")
    a, b = nodes(), nodes()

    async def scenario():
        async with websockets.connect(f"ws://127.0.0.1:{a.port}/wse") as ws:
            # Subscribe the client while node a is not in a cluster yet.
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                events = a.server.drain_inbound(16, 50)
                connected = [e for e in events if e[0] == "connect"]
                if connected:
                    a.server.subscribe_connection(connected[0][1], ["news"])
                    break
                await asyncio.sleep(0.05)
            a.join(b)
            b.join(a)
            assert wait_for(lambda: a.peers == 1 and b.peers == 1)
            await asyncio.sleep(0.5)  # the link's RESYNC carries the topic
            b.server.broadcast("news", '{"t":"news","p":{"n":1}}')
            while True:
                frame = await asyncio.wait_for(ws.recv(), timeout=5)
                text = frame if isinstance(frame, str) else frame.decode(errors="replace")
                if '"news"' in text:
                    return json.loads(text[text.index("{") :])

    message = asyncio.run(scenario())
    assert message["p"] == {"n": 1}


# ---------------------------------------------------------------------------
# Where links come in
# ---------------------------------------------------------------------------


def test_a_listening_option_is_required(nodes):
    node = nodes()
    with pytest.raises(ValueError, match="cluster_port"):
        node.server.connect_cluster(peers=[], cluster_secret=SECRET)
    with pytest.raises(ValueError, match="exclude"):
        node.server.connect_cluster(
            peers=[], cluster_port=_next_port(), single_port=True, cluster_secret=SECRET
        )
    with pytest.raises(ValueError, match="cluster_host needs cluster_port"):
        node.server.connect_cluster(
            peers=[], single_port=True, cluster_host="127.0.0.1", cluster_secret=SECRET
        )


def test_a_cluster_port_in_use_is_an_error_of_the_call(nodes):
    a, b = nodes(), nodes()
    a.join()
    with pytest.raises(OSError, match="cannot listen"):
        b.server.connect_cluster(peers=[], cluster_port=a.cluster_port, cluster_secret=SECRET)


def test_the_websocket_port_refuses_cluster_links_without_single_port(nodes):
    import socket
    import struct

    node = nodes()
    node.join()
    # A HELLO-shaped frame: a 4-byte length, then the header. Refused, the
    # connection is closed without a HELLO back.
    body = b"\x00" * 24
    with socket.create_connection(("127.0.0.1", node.port), timeout=5) as sock:
        sock.sendall(struct.pack(">I", len(body)) + body)
        sock.settimeout(5)
        try:
            answer = sock.recv(64)
        except ConnectionResetError:
            answer = b""
        assert answer == b""
    assert node.peers == 0


def test_single_port_cluster_forms_and_shares_presence(nodes):
    websockets = pytest.importorskip("websockets")
    from tests.conftest import JWT_AUDIENCE, JWT_ISSUER, JWT_SECRET, make_token

    options = {
        "single_port": True,
        "presence_enabled": True,
        "jwt_secret": JWT_SECRET,
        "jwt_issuer": JWT_ISSUER,
        "jwt_audience": JWT_AUDIENCE,
    }
    a, b = nodes(**options), nodes(**options)
    a.join(b)
    b.join(a)
    assert wait_for(lambda: a.peers == 1 and b.peers == 1)

    async def scenario():
        async with websockets.connect(
            f"ws://127.0.0.1:{a.port}/wse",
            additional_headers={"Authorization": f"Bearer {make_token('alice')}"},
        ):
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                joined = [e for e in a.server.drain_inbound(16, 50) if e[0] == "auth_connect"]
                if joined:
                    a.server.subscribe_connection(joined[0][1], ["room"], {"status": "here"})
                    break
                await asyncio.sleep(0.05)
            return await asyncio.to_thread(wait_for, lambda: "alice" in b.server.presence("room"))

    assert asyncio.run(scenario())


# ---------------------------------------------------------------------------
# The cluster's secret
# ---------------------------------------------------------------------------


def test_the_cluster_needs_its_secret_or_certificates(nodes, monkeypatch):
    for name in (
        "WSE_CLUSTER_SECRET",
        "WSE_CLUSTER_TLS_CERT",
        "WSE_CLUSTER_TLS_KEY",
        "WSE_CLUSTER_TLS_CA",
    ):
        monkeypatch.delenv(name, raising=False)
    node = nodes()
    with pytest.raises(ValueError, match="secret"):
        node.server.connect_cluster(peers=[], cluster_port=_next_port())
    with pytest.raises(ValueError, match="at least 32"):
        node.server.connect_cluster(peers=[], cluster_port=_next_port(), cluster_secret="short")
    with pytest.raises(ValueError, match="not both"):
        node.server.connect_cluster(
            peers=[],
            cluster_port=_next_port(),
            cluster_secret=SECRET,
            tls_cert="node.pem",
            tls_key="node.key",
            tls_ca="ca.pem",
        )
    with pytest.raises(TypeError):
        node.server.connect_cluster(peers=[], cluster_port=_next_port(), cluster_secret=42)


def test_nodes_link_with_the_secret_from_the_environment(nodes, monkeypatch):
    monkeypatch.setenv("WSE_CLUSTER_SECRET", SECRET)
    a, b = nodes(), nodes()
    for node, peers in ((a, []), (b, [a])):
        node.server.connect_cluster(peers=[p.addr for p in peers], cluster_port=node.cluster_port)
    assert wait_for(lambda: a.peers == 1 and b.peers == 1)


def test_a_node_with_another_secret_does_not_link(nodes):
    a, b = nodes(), nodes()
    a.join()
    b.join(a, cluster_secret=b"another secret, also of at least 32 bytes")
    time.sleep(3.0)
    assert (a.peers, b.peers) == (0, 0)


def test_a_plaintext_peer_is_refused(nodes):
    import socket
    import struct

    node = nodes()
    node.join()
    # What a node of a plaintext cluster sends first: a HELLO frame. The
    # cluster port speaks TLS only and answers nothing.
    hello = struct.pack("<BBHI", 0x04, 0, 0, 12) + b"WSE\x00" + struct.pack("<HHI", 1, 0, 15)
    with socket.create_connection(("127.0.0.1", node.cluster_port), timeout=5) as sock:
        sock.sendall(struct.pack(">I", len(hello)) + hello)
        sock.settimeout(5)
        try:
            answer = sock.recv(64)
        except (ConnectionResetError, TimeoutError):
            answer = b""
        assert b"WSE" not in answer
    assert node.peers == 0


# ---------------------------------------------------------------------------
# Mutual TLS
# ---------------------------------------------------------------------------


def _issue(tmp_path, *, san):
    x509 = pytest.importorskip("cryptography.x509")
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    now = datetime.datetime.now(datetime.UTC)

    def name(cn):
        return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])

    def write(path, key, cert):
        path.with_suffix(".key").write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        path.with_suffix(".pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca = (
        x509.CertificateBuilder()
        .subject_name(name("WSE test CA"))
        .issuer_name(name("WSE test CA"))
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    write(tmp_path / "ca", ca_key, ca)

    key = ec.generate_private_key(ec.SECP256R1())
    builder = (
        x509.CertificateBuilder()
        .subject_name(name("wse-node"))
        .issuer_name(ca.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.ExtendedKeyUsage(
                [ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]
            ),
            critical=False,
        )
    )
    if san:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
    write(tmp_path / "node", key, builder.sign(ca_key, hashes.SHA256()))
    return {
        "tls_cert": str(tmp_path / "node.pem"),
        "tls_key": str(tmp_path / "node.key"),
        "tls_ca": str(tmp_path / "ca.pem"),
    }


def test_mutual_tls_cluster_forms(nodes, tmp_path):
    tls = _issue(tmp_path, san=True)
    a, b = nodes(), nodes()
    a.join(b, **tls)
    b.join(a, **tls)
    assert wait_for(lambda: a.peers == 1 and b.peers == 1)


def test_mutual_tls_refuses_certificate_without_address(nodes, tmp_path):
    tls = _issue(tmp_path, san=False)
    a, b = nodes(), nodes()
    a.join(b, **tls)
    b.join(a, **tls)
    assert not wait_for(lambda: a.peers or b.peers, timeout=3.0)


def test_the_application_hears_of_a_user_who_joins_and_leaves_on_another_node(nodes):
    websockets = pytest.importorskip("websockets")
    from tests.conftest import JWT_AUDIENCE, JWT_ISSUER, JWT_SECRET, make_token

    options = {
        "presence_enabled": True,
        "jwt_secret": JWT_SECRET,
        "jwt_issuer": JWT_ISSUER,
        "jwt_audience": JWT_AUDIENCE,
    }
    a, b = nodes(**options), nodes(**options)
    a.join(b)
    b.join(a)
    assert wait_for(lambda: a.peers == 1 and b.peers == 1)
    heard: list[tuple[str, str]] = []

    def hear(node):
        for event in node.server.drain_inbound(16, 50):
            if event[0] in ("presence_join", "presence_leave"):
                heard.append((event[0], event[2]["user_id"]))

    async def scenario():
        ws = await websockets.connect(
            f"ws://127.0.0.1:{b.port}/wse",
            additional_headers={"Authorization": f"Bearer {make_token('alice')}"},
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            joined = [e for e in b.server.drain_inbound(16, 50) if e[0] == "auth_connect"]
            if joined:
                b.server.subscribe_connection(joined[0][1], ["room"], {"here": True})
                break
            await asyncio.sleep(0.05)
        await asyncio.to_thread(wait_for, lambda: hear(a) or ("presence_join", "alice") in heard)
        await ws.close()
        await asyncio.to_thread(wait_for, lambda: hear(a) or ("presence_leave", "alice") in heard)

    asyncio.run(scenario())
    assert heard == [("presence_join", "alice"), ("presence_leave", "alice")]


def test_a_node_started_again_without_its_peers_lists_none_of_their_users(nodes):
    websockets = pytest.importorskip("websockets")
    from tests.conftest import JWT_AUDIENCE, JWT_ISSUER, JWT_SECRET, make_token

    options = {
        "presence_enabled": True,
        "jwt_secret": JWT_SECRET,
        "jwt_issuer": JWT_ISSUER,
        "jwt_audience": JWT_AUDIENCE,
    }
    a, b = nodes(**options), nodes(**options)
    a.join(b)
    b.join(a)
    assert wait_for(lambda: a.peers == 1 and b.peers == 1)

    async def scenario():
        ws = await websockets.connect(
            f"ws://127.0.0.1:{b.port}/wse",
            additional_headers={"Authorization": f"Bearer {make_token('alice')}"},
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            joined = [e for e in b.server.drain_inbound(16, 50) if e[0] == "auth_connect"]
            if joined:
                b.server.subscribe_connection(joined[0][1], ["room"], {"here": True})
                break
            await asyncio.sleep(0.05)
        assert await asyncio.to_thread(wait_for, lambda: "alice" in a.server.presence("room"))
        # Started again, in no cluster: only its own users are present.
        a.server.stop()
        a.server.start()
        listed = "alice" in a.server.presence("room")
        await ws.close()
        return listed

    assert asyncio.run(scenario()) is False


def test_a_user_who_left_every_node_is_gone_from_all(nodes):
    websockets = pytest.importorskip("websockets")
    from tests.conftest import JWT_AUDIENCE, JWT_ISSUER, JWT_SECRET, make_token

    options = {
        "presence_enabled": True,
        "jwt_secret": JWT_SECRET,
        "jwt_issuer": JWT_ISSUER,
        "jwt_audience": JWT_AUDIENCE,
    }
    a, b = nodes(**options), nodes(**options)
    a.join(b)
    b.join(a)
    assert wait_for(lambda: a.peers == 1 and b.peers == 1)

    async def tab(node):
        ws = await websockets.connect(
            f"ws://127.0.0.1:{node.port}/wse",
            additional_headers={"Authorization": f"Bearer {make_token('alice')}"},
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            joined = [e for e in node.server.drain_inbound(16, 50) if e[0] == "auth_connect"]
            if joined:
                node.server.subscribe_connection(joined[0][1], ["room"], {"here": True})
                return ws
            await asyncio.sleep(0.05)
        raise AssertionError("no auth_connect")

    def listed(node):
        return "alice" in node.server.presence("room")

    async def scenario():
        on_a = await tab(a)
        on_b = await tab(b)
        assert await asyncio.to_thread(wait_for, lambda: listed(a) and listed(b))
        await on_a.close()
        # Still on b: both nodes keep listing her.
        await asyncio.sleep(0.5)
        assert listed(a) and listed(b)
        await on_b.close()
        return await asyncio.to_thread(wait_for, lambda: not listed(a) and not listed(b))

    assert asyncio.run(scenario())


def test_a_topic_published_on_two_nodes_recovers_both(nodes):
    """Each node stamps its own stream of the topic: a client back from a gap
    gets what it missed of both, in the order it came to its node."""
    websockets = pytest.importorskip("websockets")
    a, b = nodes(recovery=True), nodes(recovery=True)
    a.join(b)
    b.join(a)
    assert wait_for(lambda: a.peers == 1 and b.peers == 1)

    def publish(node, text):
        node.server.broadcast("room", json.dumps({"t": "chat", "p": {"text": text}}))
        time.sleep(0.1)  # in this order on b, relayed or not

    async def scenario():
        async with websockets.connect(f"ws://127.0.0.1:{b.port}/wse") as ws:
            await subscribe_next_client(b, ["room"])
            await asyncio.sleep(0.5)  # the SUB reaches a
            for node, text in [(a, "a1"), (b, "b1"), (a, "a2"), (b, "b2")]:
                publish(node, text)
            seen = await messages_of_type(ws, "chat", 4)
        assert [m["p"]["text"] for m in seen] == ["a1", "b1", "a2", "b2"]
        last = seen[-1]

        for node, text in [(a, "a3"), (b, "b3"), (a, "a4"), (b, "b4")]:
            publish(node, text)

        async with websockets.connect(f"ws://127.0.0.1:{b.port}/wse") as ws:
            deadline = time.monotonic() + 5
            conn_id = None
            while conn_id is None and time.monotonic() < deadline:
                for event in b.server.drain_inbound(16, 50):
                    if event[0] == "connect":
                        conn_id = event[1]
                await asyncio.sleep(0.05)
            result = b.server.subscribe_with_recovery(
                conn_id,
                ["room"],
                recover=True,
                positions={"room": {"epoch": last["e"], "offset": last["o"]}},
            )
            replayed = await messages_of_type(ws, "chat", 4)
        return result, replayed

    result, replayed = asyncio.run(scenario())
    assert result["topics"]["room"]["recovered"] is True
    assert [m["p"]["text"] for m in replayed] == ["a3", "b3", "a4", "b4"]


def test_a_client_that_moves_node_gets_each_stream_from_its_own_offset(nodes):
    """Nodes have a topic's streams in their own order: a client that names
    its offset in each loses nothing when it comes back on another node than
    the one whose order it saw."""
    websockets = pytest.importorskip("websockets")
    a, b = nodes(recovery=True), nodes(recovery=True)
    a.join(b)
    b.join(a)
    assert wait_for(lambda: a.peers == 1 and b.peers == 1)

    def publish(node, text):
        node.server.broadcast("room", json.dumps({"t": "chat", "p": {"text": text}}))
        time.sleep(0.1)  # in this order on b, relayed or not

    async def scenario():
        async with websockets.connect(f"ws://127.0.0.1:{b.port}/wse") as ws:
            await subscribe_next_client(b, ["room"])
            await asyncio.sleep(0.5)  # the SUB reaches a
            publish(a, "a0")
            publish(b, "b0")
            # b has b1 before a1. A client on a node that had a1 first got
            # a1 there, and lost its link before b1.
            publish(b, "b1")
            publish(a, "a1")
            seen = {m["p"]["text"]: m for m in await messages_of_type(ws, "chat", 4)}
        a1, b0 = seen["a1"], seen["b0"]
        position = {
            "epoch": a1["e"],
            "offset": a1["o"],
            "streams": {a1["e"]: a1["o"], b0["e"]: b0["o"]},
        }
        async with websockets.connect(f"ws://127.0.0.1:{b.port}/wse") as ws:
            deadline = time.monotonic() + 5
            conn_id = None
            while conn_id is None and time.monotonic() < deadline:
                for event in b.server.drain_inbound(16, 50):
                    if event[0] == "connect":
                        conn_id = event[1]
                await asyncio.sleep(0.05)
            result = b.server.subscribe_with_recovery(
                conn_id, ["room"], recover=True, positions={"room": position}
            )
            replayed = await messages_of_type(ws, "chat", 1)
        return result, replayed, a1, seen["b1"]

    result, replayed, a1, b1 = asyncio.run(scenario())
    room = result["topics"]["room"]
    assert room["recovered"] is True
    assert [m["p"]["text"] for m in replayed] == ["b1"]
    assert room["streams"] == {a1["e"]: a1["o"], b1["e"]: b1["o"]}
