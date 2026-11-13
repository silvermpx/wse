"""Two-node clusters on loopback: forming, peers leaving, mutual TLS.

Instance ids are time-ordered and made in connect_cluster(), so the node that
joins first holds the lower id. Each scenario names which node dials.
"""

import datetime
import ipaddress
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
    def __init__(self):
        self.server = RustWSEServer("127.0.0.1", _next_port(), max_connections=10)
        self.server.enable_drain_mode()
        self.server.start()
        self.cluster_port = _next_port()
        self.running = True

    @property
    def addr(self):
        return f"127.0.0.1:{self.cluster_port}"

    @property
    def peers(self):
        return self.server.cluster_peers_count()

    def join(self, *peers, **tls):
        self.server.connect_cluster(
            peers=[p.addr for p in peers], cluster_port=self.cluster_port, **tls
        )

    def stop(self):
        if self.running:
            self.running = False
            self.server.stop()


@pytest.fixture()
def nodes():
    made = []

    def make():
        node = Node()
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
