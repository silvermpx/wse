"""Two-node clusters on loopback: forming, peers leaving, mutual TLS.

Instance ids are time-ordered and made in connect_cluster(), so the node that
joins first holds the lower id. Each scenario names which node dials.
"""

import asyncio
import datetime
import ipaddress
import json
import time

import pytest

from tests.conftest import _next_port
from wse_server import RustWSEServer


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

    def join(self, *peers, **tls):
        if self.single_port:
            listen = {"single_port": True}
        else:
            listen = {"cluster_port": self.cluster_port}
        self.server.connect_cluster(peers=[p.addr for p in peers], **listen, **tls)

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


async def subscribe_next_client(node, topics):
    """Wait for the next client's connect event on `node` and subscribe it."""
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        for event in node.server.drain_inbound(16, 50):
            if event[0] == "connect":
                node.server.subscribe_connection(event[1], topics)
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
        node.server.connect_cluster(peers=[])
    with pytest.raises(ValueError, match="exclude"):
        node.server.connect_cluster(peers=[], cluster_port=_next_port(), single_port=True)
    with pytest.raises(ValueError, match="cluster_host needs cluster_port"):
        node.server.connect_cluster(peers=[], single_port=True, cluster_host="127.0.0.1")


def test_a_cluster_port_in_use_is_an_error_of_the_call(nodes):
    a, b = nodes(), nodes()
    a.join()
    with pytest.raises(OSError, match="cannot listen"):
        b.server.connect_cluster(peers=[], cluster_port=a.cluster_port)


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
