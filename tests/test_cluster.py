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
import socket
import time
from types import SimpleNamespace

import pytest

from tests.conftest import _next_port
from tests.test_transport import metric
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
    through it and refuse new ones for a while, or let its links go quiet."""

    def __init__(self, target):
        self.target = target
        self.port = _next_port()
        self.open = True
        self.links = set()
        self.frozen = set()
        self.quiet = asyncio.Event()

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
                    if pair in self.frozen:
                        await self.quiet.wait()
                        return
                    dst.write(data)
                    await dst.drain()
                if pair in self.frozen:
                    await self.quiet.wait()
                    return
                # The other side closed: so does this one, after what it sent.
                if dst.can_write_eof():
                    dst.write_eof()
            except ConnectionError:
                if pair not in self.frozen:
                    dst.transport.abort()

        await asyncio.gather(pipe(down_reader, up_writer), pipe(up_reader, down_writer))
        for writer in pair:
            writer.close()
        self.links.discard(pair)

    def cut(self):
        self.open = False
        for down, up in list(self.links):
            down.transport.abort()
            up.transport.abort()

    def freeze(self):
        """Its links go quiet, as if a far end were gone without a word:
        nothing more crosses them, and nothing closes them, until close().
        New links go through."""
        self.frozen.update(self.links)

    def close(self):
        self.cut()
        self.quiet.set()
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


def test_a_stream_a_pattern_matches_begun_while_a_link_was_down_arrives_after_it(nodes):
    """A client holds a pattern; while the link between the nodes is down, the
    origin publishes to a new topic it matches: the client gets that stream
    from its first message once the link is back, and the established stream
    with nothing repeated."""
    websockets = pytest.importorskip("websockets")
    origin, relay = nodes(recovery=True), nodes(recovery=True)
    origin.join()

    def publish(topic, n):
        origin.server.broadcast(topic, json.dumps({"t": "price", "p": {"n": n}}))

    async def scenario():
        blink = Blink(origin)
        await blink.start()
        try:
            relay.join(blink)
            await until(lambda: origin.peers == 1 and relay.peers == 1)
            async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as live:
                await subscribe_next_client(relay, ["prices:*"])
                await asyncio.sleep(0.4)  # the SUB reaches the origin
                publish("prices:established", 0)
                got = await messages_of_type(live, "price", 1)
                blink.cut()
                await until(lambda: origin.peers == 0 and relay.peers == 0)
                publish("prices:new", 1)
                publish("prices:established", 2)
                blink.open = True
                await until(lambda: origin.peers == 1 and relay.peers == 1, timeout=20.0)
                await asyncio.sleep(0.3)
                publish("prices:new", 3)
                while got[-1]["p"]["n"] != 3:
                    got.extend(await messages_of_type(live, "price", 1))
        finally:
            blink.close()
        return got

    got = asyncio.run(scenario())
    seen = sorted((m["tp"], m["p"]["n"]) for m in got)
    assert seen == [
        ("prices:established", 0),
        ("prices:established", 2),
        ("prices:new", 1),
        ("prices:new", 3),
    ], seen
    new = [m["o"] for m in got if m["tp"] == "prices:new"]
    assert new == [0, 1], "the new stream from its first message, in order"


def test_a_message_no_node_kept_while_a_link_was_down_is_never_said_recovered(nodes):
    """While the link between the nodes is down the origin, with no room
    left for history, publishes to a topic the other node has: once the link
    is back, a client of that node whose last message came before it is told
    it was not recovered, never that it missed nothing."""
    websockets = pytest.importorskip("websockets")
    origin = nodes(recovery=True, recovery_memory_budget=1)
    relay = nodes(recovery=True)
    origin.join()

    def message(topic, n):
        return json.dumps({"t": topic, "p": {"n": n}})

    async def scenario():
        blink = Blink(origin)
        await blink.start()
        try:
            relay.join(blink)
            await until(lambda: origin.peers == 1 and relay.peers == 1)
            relay_url = f"ws://127.0.0.1:{relay.port}/wse"
            origin_url = f"ws://127.0.0.1:{origin.port}/wse"
            async with (
                websockets.connect(relay_url) as live,
                websockets.connect(origin_url) as source,
            ):
                await subscribe_next_client(relay, ["news", "barrier"])
                await subscribe_next_client(origin, ["news", "other"])
                await asyncio.sleep(0.35)  # the SUB reaches the origin
                relay.server.broadcast_local("news", message("news", 1))
                (anchor,) = await messages_of_type(live, "news", 1)
                blink.cut()
                await until(lambda: origin.peers == 0 and relay.peers == 0)
                # The origin's budget is used up: what it publishes has no position.
                origin.server.broadcast_local("other", message("other", 0))
                await messages_of_type(source, "other", 1)
                origin.server.broadcast("news", message("news", 2))
                (unkept,) = await messages_of_type(source, "news", 1)
                assert unkept["p"]["n"] == 2 and "o" not in unkept
                blink.open = True
                await until(lambda: origin.peers == 1 and relay.peers == 1, timeout=20.0)
                await asyncio.sleep(0.5)
                # The nodes talk again: what the origin publishes arrives.
                origin.server.broadcast("barrier", message("barrier", 3))
                await messages_of_type(live, "barrier", 1)
            answers = []
            for streams in ({}, {"streams": {anchor["e"]: anchor["o"]}}):
                async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse"):
                    conn_id = await next_connect(relay)
                    past = {"epoch": anchor["e"], "offset": anchor["o"], **streams}
                    answer = relay.server.subscribe_with_recovery(
                        conn_id, ["news"], recover=True, positions={"news": past}
                    )
                    answers.append(answer["topics"]["news"])
        finally:
            blink.close()
        return answers

    for answer in asyncio.run(scenario()):
        assert answer["recovered"] is False, answer


def test_a_message_a_node_had_no_room_to_keep_is_not_delivered_again_after_a_lost_link(nodes):
    """A node with no room left for history delivers what a peer relays to it
    live; once a lost link is back, the catch-up brings what it missed, and
    nothing it delivered already a second time."""
    websockets = pytest.importorskip("websockets")
    origin = nodes(recovery=True)
    relay = nodes(recovery=True, recovery_memory_budget=1)
    origin.join()

    def price(n):
        return json.dumps({"t": "price", "p": {"n": n}})

    async def scenario():
        blink = Blink(origin)
        await blink.start()
        try:
            relay.join(blink)
            await until(lambda: origin.peers == 1 and relay.peers == 1)
            async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as live:
                await subscribe_next_client(relay, ["prices:*", "setup"])
                await asyncio.sleep(0.35)  # the SUB reaches the origin
                # The relay's budget is used up: what is relayed to it is not kept.
                relay.server.broadcast_local("setup", json.dumps({"t": "setup", "p": {}}))
                await messages_of_type(live, "setup", 1)
                origin.server.broadcast("prices:open", price(0))
                got = await messages_of_type(live, "price", 1)
                blink.cut()
                await until(lambda: origin.peers == 0 and relay.peers == 0)
                origin.server.broadcast("prices:open", price(1))
                await asyncio.sleep(0.3)
                blink.open = True
                await until(lambda: origin.peers == 1 and relay.peers == 1, timeout=20.0)
                await asyncio.sleep(0.4)
                origin.server.broadcast("prices:open", price(2))
                while got[-1]["p"]["n"] != 2:
                    got.extend(await messages_of_type(live, "price", 1))
        finally:
            blink.close()
        return got

    got = asyncio.run(scenario())
    assert [message["p"]["n"] for message in got] == [0, 1, 2], got


def test_a_message_published_to_one_nodes_subscribers_stays_there_through_a_catch_up(nodes):
    """What a node publishes to its own subscribers only is not sent to
    another node when a link comes up, with the catch-up of what it missed."""
    websockets = pytest.importorskip("websockets")
    origin = nodes(recovery=True)
    other = nodes(recovery=True)

    def message(topic, n):
        return json.dumps({"t": topic, "p": {"n": n}})

    async def scenario():
        async with websockets.connect(f"ws://127.0.0.1:{other.port}/wse") as live:
            await subscribe_next_client(other, ["news", "barrier"])
            origin.server.broadcast_local("news", message("news", 1))
            origin.join()
            other.join(origin)
            await until(lambda: origin.peers == 1 and other.peers == 1)
            await asyncio.sleep(0.5)
            origin.server.broadcast("barrier", message("barrier", 2))
            got = []
            while not got or got[-1].get("t") != "barrier":
                frame = await asyncio.wait_for(live.recv(), 5.0)
                text = frame if isinstance(frame, str) else frame.decode(errors="replace")
                got.append(json.loads(text[text.index("{") :]))
        return [entry.get("t") for entry in got]

    seen = asyncio.run(scenario())
    assert "news" not in seen, seen


def _recovered_both_ways(relay, anchor, websockets):
    """What a client of the relay whose last message was `anchor` is told,
    by its scalar position and by its streams: (answer, the numbers it got)."""

    async def ask():
        told = []
        for streams in ({}, {"streams": {anchor["e"]: anchor["o"]}}):
            async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as back:
                conn_id = await next_connect(relay)
                past = {"epoch": anchor["e"], "offset": anchor["o"], **streams}
                answer = relay.server.subscribe_with_recovery(
                    conn_id, ["news"], recover=True, positions={"news": past}
                )["topics"]["news"]
                got = (
                    await messages_of_type(back, "news", answer["count"]) if answer["count"] else []
                )
                told.append((answer, [m["p"]["n"] for m in got]))
        return told

    return ask()


async def _after_an_outage(origin, relay, during, websockets, *, origin_reads=("news",)):
    """The relay's client takes a message of its own (the anchor); the link
    goes, `during(origin)` runs, the link comes back and a barrier says the
    nodes talk again. The anchor, and what the relay's client got meanwhile."""
    blink = Blink(origin)
    await blink.start()
    try:
        relay.join(blink)
        await until(lambda: origin.peers == 1 and relay.peers == 1)
        async with (
            websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as live,
            websockets.connect(f"ws://127.0.0.1:{origin.port}/wse") as source,
        ):
            await subscribe_next_client(relay, ["news", "barrier"])
            await subscribe_next_client(origin, list(origin_reads))
            await asyncio.sleep(0.35)  # the SUB reaches the origin
            relay.server.broadcast_local("news", json.dumps({"t": "news", "p": {"n": -1}}))
            (anchor,) = await messages_of_type(live, "news", 1)
            blink.cut()
            await until(lambda: origin.peers == 0 and relay.peers == 0)
            await during(origin, source)
            blink.open = True
            await until(lambda: origin.peers == 1 and relay.peers == 1, timeout=20.0)
            await asyncio.sleep(0.4)
            origin.server.broadcast("barrier", json.dumps({"t": "barrier", "p": {"n": 0}}))
            got = []
            while True:
                frame = await asyncio.wait_for(live.recv(), 5)
                text = frame if isinstance(frame, str) else frame.decode(errors="replace")
                message = json.loads(text[text.index("{") :])
                if message.get("t") == "barrier":
                    break
                if message.get("t") == "news":
                    got.append(message["p"]["n"])
            return anchor, got, blink
    except BaseException:
        blink.close()
        raise


def test_a_stream_begun_while_a_link_was_down_and_longer_than_its_history_is_never_said_recovered(
    nodes,
):
    """Seventeen messages, of a stream the other node never heard of, into a
    history of sixteen: the other node's clients get the sixteen kept, and
    one whose last message came before them is not told it missed nothing."""
    websockets = pytest.importorskip("websockets")
    origin = nodes(recovery=True, recovery_buffer_size=16)
    relay = nodes(recovery=True)
    origin.join()

    async def seventeen(origin, source):
        for n in range(17):
            origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": n}}))
        await messages_of_type(source, "news", 17)

    async def scenario():
        anchor, got, blink = await _after_an_outage(origin, relay, seventeen, websockets)
        try:
            return got, await _recovered_both_ways(relay, anchor, websockets)
        finally:
            blink.close()

    got, told = asyncio.run(scenario())
    assert got == list(range(1, 17))
    for answer, numbers in told:
        assert answer["recovered"] is False or numbers == list(range(17)), answer


def test_a_message_for_the_cluster_keeps_its_place_whatever_is_published_locally(nodes):
    """While the link is down, one message for the cluster, then sixteen for
    the origin's own subscribers, into a history of sixteen: the local ones
    have a stream of their own, and the other node recovers the one."""
    websockets = pytest.importorskip("websockets")
    origin = nodes(recovery=True, recovery_buffer_size=16)
    relay = nodes(recovery=True)
    origin.join()

    async def one_then_local(origin, source):
        origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 0}}))
        for n in range(1, 17):
            origin.server.broadcast_local("news", json.dumps({"t": "news", "p": {"n": n}}))
        await messages_of_type(source, "news", 17)

    async def scenario():
        anchor, got, blink = await _after_an_outage(origin, relay, one_then_local, websockets)
        try:
            return got, await _recovered_both_ways(relay, anchor, websockets)
        finally:
            blink.close()

    got, told = asyncio.run(scenario())
    assert got == [0]
    for answer, numbers in told:
        assert answer["recovered"] is True and numbers == [0], answer


def test_a_message_published_locally_between_two_for_the_cluster_leaves_no_hole(nodes):
    """The other node gets the two for the cluster, and recovers the second
    from the first: the local one is no gap in the cluster's stream."""
    websockets = pytest.importorskip("websockets")
    origin, relay = nodes(recovery=True), nodes(recovery=True)
    origin.join()
    relay.join(origin)

    async def scenario():
        await until(lambda: origin.peers == 1 and relay.peers == 1)
        async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as live:
            await subscribe_next_client(relay, ["news"])
            await asyncio.sleep(0.35)
            origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 0}}))
            (anchor,) = await messages_of_type(live, "news", 1)
            origin.server.broadcast_local("news", json.dumps({"t": "news", "p": {"n": 1}}))
            origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 2}}))
            got = await messages_of_type(live, "news", 1)
        return (
            anchor,
            [m["p"]["n"] for m in got],
            await _recovered_both_ways(relay, anchor, websockets),
        )

    anchor, got, told = asyncio.run(scenario())
    assert got == [2]
    for answer, numbers in told:
        assert answer["recovered"] is True and numbers == [2], answer


def test_a_second_empty_outage_tells_no_loss_again(nodes):
    """A loss the other node was told of once is not told again after a
    later outage in which nothing was lost: a client past it recovers."""
    websockets = pytest.importorskip("websockets")
    origin = nodes(recovery=True, recovery_memory_budget=1)
    relay = nodes(recovery=True)
    origin.join()

    async def unkept(origin, source):
        origin.server.broadcast_local("other", json.dumps({"t": "other", "p": {}}))
        await messages_of_type(source, "other", 1)
        origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 2}}))
        await messages_of_type(source, "news", 1)

    async def scenario():
        anchor, _, blink = await _after_an_outage(
            origin, relay, unkept, websockets, origin_reads=("news", "other")
        )
        try:
            first = await _recovered_both_ways(relay, anchor, websockets)
            async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as live:
                await subscribe_next_client(relay, ["news"])
                relay.server.broadcast_local("news", json.dumps({"t": "news", "p": {"n": 4}}))
                (later,) = await messages_of_type(live, "news", 1)
            blink.cut()
            await until(lambda: origin.peers == 0 and relay.peers == 0)
            blink.open = True
            await until(lambda: origin.peers == 1 and relay.peers == 1, timeout=20.0)
            await asyncio.sleep(0.4)
            return first, await _recovered_both_ways(relay, later, websockets)
        finally:
            blink.close()

    first, second = asyncio.run(scenario())
    assert [answer["recovered"] for answer, _ in first] == [False, False]
    assert [answer["recovered"] for answer, _ in second] == [True, True], second


def test_a_loss_whose_record_the_origin_let_go_is_still_never_said_recovered(nodes):
    """The origin could not keep a message while the link was down, and the
    link came back only after the origin let its record of that go: the
    other node's client is not told it missed nothing."""
    websockets = pytest.importorskip("websockets")
    origin = nodes(recovery=True, recovery_ttl=1, recovery_memory_budget=1)
    relay = nodes(recovery=True, recovery_ttl=1)
    origin.join()

    async def unkept_then_forgotten(origin, source):
        origin.server.broadcast_local("other", json.dumps({"t": "other", "p": {}}))
        await messages_of_type(source, "other", 1)
        origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 2}}))
        await messages_of_type(source, "news", 1)
        # The origin's cleanup runs every 30 s: its history and its record go.
        deadline = time.monotonic() + 70
        while metric(origin.server, "wse_recovery_bytes") and time.monotonic() < deadline:
            # The relay's own history stays fresh meanwhile.
            relay.server.broadcast_local("news", json.dumps({"t": "news", "p": {"n": 10}}))
            await asyncio.sleep(0.5)
        await asyncio.sleep(0.5)

    async def scenario():
        anchor, _, blink = await _after_an_outage(
            origin, relay, unkept_then_forgotten, websockets, origin_reads=("news", "other")
        )
        try:
            return await _recovered_both_ways(relay, anchor, websockets)
        finally:
            blink.close()

    for answer, numbers in asyncio.run(scenario()):
        assert answer["recovered"] is False or 2 in numbers, answer


def test_a_relayed_stream_whose_history_went_is_not_delivered_again_after_a_lost_link(nodes):
    """A client stays subscribed through a lost link while the relay lets its
    history of the stream go: the catch-up brings what the client did not
    have, and nothing it had a second time."""
    websockets = pytest.importorskip("websockets")
    origin = nodes(recovery=True, recovery_ttl=1)
    relay = nodes(recovery=True, recovery_ttl=1)
    origin.join()

    def price(n):
        return json.dumps({"t": "price", "p": {"n": n}})

    async def scenario():
        blink = Blink(origin)
        await blink.start()
        try:
            relay.join(blink)
            await until(lambda: origin.peers == 1 and relay.peers == 1)
            async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as live:
                await subscribe_next_client(relay, ["prices:*"])
                await asyncio.sleep(0.35)
                origin.server.broadcast("prices:open", price(0))
                got = await messages_of_type(live, "price", 1)
                blink.cut()
                await until(lambda: origin.peers == 0 and relay.peers == 0)
                n = 1
                deadline = time.monotonic() + 70
                # The relay lets its idle history of the stream go.
                while metric(relay.server, "wse_recovery_bytes") and time.monotonic() < deadline:
                    origin.server.broadcast("prices:open", price(n))
                    n += 1
                    await asyncio.sleep(0.5)
                blink.open = True
                await until(lambda: origin.peers == 1 and relay.peers == 1, timeout=20.0)
                await asyncio.sleep(0.4)
                origin.server.broadcast("prices:open", price(n))
                while got[-1]["p"]["n"] != n:
                    got.extend(await messages_of_type(live, "price", 1))
            return [m["p"]["n"] for m in got], n
        finally:
            blink.close()

    numbers, last = asyncio.run(scenario())
    assert numbers == list(range(last + 1)), numbers


def test_a_node_back_anew_after_a_lost_link_is_never_taken_to_have_lost_nothing(nodes):
    """The origin publishes while its link to the other node is down, and
    goes before the link is back; a new node comes up in its place. A client
    of the other node whose last message came from the origin before the
    outage is not told it missed nothing."""
    websockets = pytest.importorskip("websockets")
    origin = nodes(recovery=True)
    relay = nodes(recovery=True)
    origin.join()

    async def scenario():
        blink = Blink(origin)
        await blink.start()
        try:
            relay.join(blink)
            await until(lambda: origin.peers == 1 and relay.peers == 1)
            async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as live:
                await subscribe_next_client(relay, ["news", "barrier"])
                await asyncio.sleep(0.35)  # the SUB reaches the origin
                origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 1}}))
                (anchor,) = await messages_of_type(live, "news", 1)
                blink.cut()
                await until(lambda: origin.peers == 0 and relay.peers == 0)
                origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 2}}))
                await asyncio.sleep(0.2)
                # Its history goes with it; the other node hears no SHUTDOWN.
                origin.stop()
                anew = nodes(recovery=True)
                anew.join()
                blink.target = anew
                blink.open = True
                await until(lambda: anew.peers == 1 and relay.peers == 1, timeout=20.0)
                await asyncio.sleep(0.4)
                anew.server.broadcast("barrier", json.dumps({"t": "barrier", "p": {"n": 3}}))
                await messages_of_type(live, "barrier", 1)
            return await _recovered_both_ways(relay, anchor, websockets)
        finally:
            blink.close()

    for answer, numbers in asyncio.run(scenario()):
        assert answer["recovered"] is False or 2 in numbers, answer


def test_a_node_that_stopped_saying_so_and_came_back_anew_lost_nothing(nodes):
    """The origin stops while its link is up (it says SHUTDOWN after what it
    sent), and a new node comes up in its place: a client of the other node
    recovers as before."""
    websockets = pytest.importorskip("websockets")
    origin = nodes(recovery=True)
    relay = nodes(recovery=True)
    origin.join()

    async def scenario():
        blink = Blink(origin)
        await blink.start()
        try:
            relay.join(blink)
            await until(lambda: origin.peers == 1 and relay.peers == 1)
            async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as live:
                await subscribe_next_client(relay, ["news", "barrier"])
                await asyncio.sleep(0.35)  # the SUB reaches the origin
                origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 1}}))
                (anchor,) = await messages_of_type(live, "news", 1)
                origin.stop()
                await until(lambda: relay.peers == 0)
                anew = nodes(recovery=True)
                anew.join()
                blink.target = anew
                await until(lambda: anew.peers == 1 and relay.peers == 1, timeout=20.0)
                await asyncio.sleep(0.4)
                anew.server.broadcast("barrier", json.dumps({"t": "barrier", "p": {"n": 3}}))
                await messages_of_type(live, "barrier", 1)
            return await _recovered_both_ways(relay, anchor, websockets)
        finally:
            blink.close()

    for answer, numbers in asyncio.run(scenario()):
        assert answer["recovered"] is True and numbers == [], answer


def test_a_node_back_anew_that_dials_in_itself_is_never_taken_to_have_lost_nothing(nodes):
    """The origin is the node that dials, and so is the node that comes up at
    its address: the other node takes their links in, and still knows the
    one it had is gone with what it published while its link was down."""
    websockets = pytest.importorskip("websockets")
    relay = nodes(recovery=True)
    relay.join()
    origin = nodes(recovery=True)

    async def scenario():
        blink = Blink(relay)
        await blink.start()
        try:
            origin.join(blink)
            await until(lambda: origin.peers == 1 and relay.peers == 1)
            async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as live:
                await subscribe_next_client(relay, ["news", "barrier"])
                await asyncio.sleep(0.35)  # the SUB reaches the origin
                origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 1}}))
                (anchor,) = await messages_of_type(live, "news", 1)
                blink.cut()
                await until(lambda: origin.peers == 0 and relay.peers == 0)
                origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 2}}))
                await asyncio.sleep(0.2)
                origin.stop()
                anew = nodes(recovery=True)
                anew.cluster_port = origin.cluster_port
                blink.open = True
                anew.join(blink)
                await until(lambda: anew.peers == 1 and relay.peers == 1, timeout=20.0)
                await asyncio.sleep(0.4)
                anew.server.broadcast("barrier", json.dumps({"t": "barrier", "p": {"n": 3}}))
                await messages_of_type(live, "barrier", 1)
            return await _recovered_both_ways(relay, anchor, websockets)
        finally:
            blink.close()

    for answer, numbers in asyncio.run(scenario()):
        assert answer["recovered"] is False or 2 in numbers, answer


def _back_anew_dialing_in(nodes, *, dialed, before, after, relay=None):
    """The relay dials the origin, through a proxy, at `dialed`; the origin
    gives where it is reached as `before` says (its cluster options, of the
    proxy). Its link goes, it publishes meanwhile, it goes without SHUTDOWN,
    and the node that comes up in its place, giving `after`, dials the relay
    itself while the relay's own dials are refused. What the relay's client
    whose last message came before is told, by both forms of its position."""
    websockets = pytest.importorskip("websockets")
    origin = nodes(recovery=True)
    relay_node = nodes(recovery=True)

    async def scenario():
        route = Blink(origin)
        await route.start()
        try:
            origin.join(cluster_secret=SECRET, **before(route))
            relay_node.join(
                SimpleNamespace(addr=f"{dialed}:{route.port}"),
                cluster_secret=SECRET,
                **(relay or {}),
            )
            await until(lambda: origin.peers == 1 and relay_node.peers == 1)
            async with websockets.connect(f"ws://127.0.0.1:{relay_node.port}/wse") as live:
                await subscribe_next_client(relay_node, ["news", "barrier"])
                await asyncio.sleep(0.35)  # the SUB reaches the origin
                origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 1}}))
                (anchor,) = await messages_of_type(live, "news", 1)
                route.cut()
                await until(lambda: origin.peers == 0 and relay_node.peers == 0)
                origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 2}}))
                await asyncio.sleep(0.2)
                origin.stop()
                anew = nodes(recovery=True)
                anew.cluster_port = origin.cluster_port
                route.target = anew
                anew.join(relay_node, cluster_secret=SECRET, **after(route))
                await until(lambda: anew.peers == 1 and relay_node.peers == 1, timeout=20.0)
                await asyncio.sleep(0.4)
                anew.server.broadcast("barrier", json.dumps({"t": "barrier", "p": {"n": 3}}))
                await messages_of_type(live, "barrier", 1)
            return await _recovered_both_ways(relay_node, anchor, websockets)
        finally:
            route.close()

    return asyncio.run(scenario())


def test_a_node_dialed_by_another_name_than_it_gives_is_known_when_it_comes_back_anew(nodes):
    """The relay dials the origin by a name (localhost) while the origin
    says it is reached at its address: the node that comes up there and
    dials in itself, before the relay reaches it again, is still another
    node, gone with what it published while the link was down."""
    proxy = lambda route: {"cluster_addr": route.addr}  # noqa: E731
    for answer, numbers in _back_anew_dialing_in(
        nodes, dialed="localhost", before=proxy, after=proxy
    ):
        assert answer["recovered"] is False or 2 in numbers, answer


def _dual_stack():
    try:
        with socket.socket(socket.AF_INET6) as probe:
            probe.bind(("::", 0))
        return True
    except OSError:
        return False


def test_a_node_met_by_its_ipv4_address_is_the_same_where_a_dual_stack_listener_sees_it_mapped(
    nodes,
):
    """The relay listens on IPv4 and IPv6 at once, and the origin gives no
    address, only its port: the relay met it at 127.0.0.1, and takes the
    node that dials in from there anew (::ffff:127.0.0.1 to its listener)
    for one in the same place."""
    if not _dual_stack():
        pytest.skip("no IPv6 here")
    port_only = lambda route: {}  # noqa: E731
    results = _back_anew_dialing_in(
        nodes, dialed="127.0.0.1", before=port_only, after=port_only, relay={"cluster_host": "::"}
    )
    for answer, numbers in results:
        assert answer["recovered"] is False or 2 in numbers, answer


def test_a_node_whose_name_comes_back_in_other_letters_is_the_same_place(nodes):
    """The origin gives localhost, and the node in its place LOCALHOST: a
    host name is one whatever its case, so the relay takes the second for
    another node where the first was."""
    results = _back_anew_dialing_in(
        nodes,
        dialed="127.0.0.1",
        before=lambda route: {"cluster_addr": f"localhost:{route.port}"},
        after=lambda route: {"cluster_addr": f"LOCALHOST:{route.port}"},
    )
    for answer, numbers in results:
        assert answer["recovered"] is False or 2 in numbers, answer


def test_a_node_whose_port_comes_back_written_otherwise_is_the_same_place(nodes):
    """The origin gives its port as 044015 and the node in its place as
    44015, or the other way round: a port is a number however it is
    written, so the relay takes the second for another node where the
    first was."""
    for first, then in (("0{}", "{}"), ("{}", "0{}")):
        results = _back_anew_dialing_in(
            nodes,
            dialed="127.0.0.1",
            before=lambda route, first=first: {
                "cluster_addr": "localhost:" + first.format(route.port)
            },
            after=lambda route, then=then: {"cluster_addr": "localhost:" + then.format(route.port)},
        )
        for answer, numbers in results:
            assert answer["recovered"] is False or 2 in numbers, (first, then, answer)


def test_two_nodes_at_one_link_local_address_on_two_links_are_two_places(nodes):
    """Two nodes give one link-local address and port, on two links (zones):
    they are two nodes, both up, and the relay's clients lose nothing when
    the second comes."""
    websockets = pytest.importorskip("websockets")
    relay, first, second = (nodes(recovery=True) for _ in range(3))
    # Zones of no interface here: the links themselves go over loopback.
    zones = {index for index, _ in socket.if_nameindex()}
    one, two = 428880001, 428880002
    assert not {one, two} & zones
    options = {"cluster_secret": SECRET, "cluster_host": "127.0.0.1"}

    async def scenario():
        relay.join(**options)
        first.join(relay, cluster_addr=f"[fe80::abcd%{one}]:{first.cluster_port}", **options)
        await until(lambda: relay.peers == 1 and first.peers == 1)
        async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as live:
            await subscribe_next_client(relay, ["news", "second-node"])
            await asyncio.sleep(0.35)  # the SUB reaches the first node
            first.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 1}}))
            (anchor,) = await messages_of_type(live, "news", 1)
            second.join(relay, cluster_addr=f"[fe80::abcd%{two}]:{first.cluster_port}", **options)
            await until(lambda: relay.peers == 2 and second.peers == 1)
            await asyncio.sleep(0.4)
            second.server.broadcast("second-node", json.dumps({"t": "second-node", "p": {"n": 2}}))
            await messages_of_type(live, "second-node", 1)
        return await _recovered_both_ways(relay, anchor, websockets)

    for answer, numbers in asyncio.run(scenario()):
        assert answer["recovered"] is True and numbers == [], answer


def test_a_node_back_anew_while_its_last_link_still_looks_up_is_never_taken_to_have_lost_nothing(
    nodes,
):
    """The origin goes while the other node still holds its link, its end
    never come through, and the node that comes up at its address dials in:
    the other node takes it that the one it had went without SHUTDOWN."""
    websockets = pytest.importorskip("websockets")
    relay = nodes(recovery=True)
    relay.join()
    origin = nodes(recovery=True)

    async def scenario():
        blink = Blink(relay)
        await blink.start()
        try:
            origin.join(blink)
            await until(lambda: origin.peers == 1 and relay.peers == 1)
            async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as live:
                await subscribe_next_client(relay, ["news", "barrier"])
                await asyncio.sleep(0.35)  # the SUB reaches the origin
                origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 1}}))
                (anchor,) = await messages_of_type(live, "news", 1)
                blink.freeze()
                origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 2}}))
                await asyncio.sleep(0.2)
                origin.stop()
                assert relay.peers == 1  # the link it had still looks up
                anew = nodes(recovery=True)
                anew.cluster_port = origin.cluster_port
                anew.join(blink)
                await until(lambda: anew.peers == 1, timeout=20.0)
                await asyncio.sleep(0.4)
                anew.server.broadcast("barrier", json.dumps({"t": "barrier", "p": {"n": 3}}))
                await messages_of_type(live, "barrier", 1)
            return await _recovered_both_ways(relay, anchor, websockets)
        finally:
            blink.close()

    for answer, numbers in asyncio.run(scenario()):
        assert answer["recovered"] is False or 2 in numbers, answer


def test_a_pattern_subscriber_is_never_told_it_lost_nothing_when_a_node_is_back_anew(nodes):
    """A client subscribed by a pattern took a message this node published
    to its own subscribers; the origin publishes to that topic while its link
    is down and goes, a new node at its address: the client is not told it
    missed nothing."""
    websockets = pytest.importorskip("websockets")
    origin = nodes(recovery=True)
    relay = nodes(recovery=True)
    origin.join()

    async def scenario():
        blink = Blink(origin)
        await blink.start()
        try:
            relay.join(blink)
            await until(lambda: origin.peers == 1 and relay.peers == 1)
            async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as live:
                await subscribe_next_client(relay, ["n*", "barrier"])
                await asyncio.sleep(0.35)  # the SUB reaches the origin
                relay.server.broadcast_local("news", json.dumps({"t": "news", "p": {"n": 1}}))
                (anchor,) = await messages_of_type(live, "news", 1)
                blink.cut()
                await until(lambda: origin.peers == 0 and relay.peers == 0)
                origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 2}}))
                await asyncio.sleep(0.2)
                origin.stop()
                anew = nodes(recovery=True)
                anew.cluster_port = origin.cluster_port
                anew.join()
                blink.target = anew
                blink.open = True
                await until(lambda: anew.peers == 1 and relay.peers == 1, timeout=20.0)
                await asyncio.sleep(0.4)
                anew.server.broadcast("barrier", json.dumps({"t": "barrier", "p": {"n": 3}}))
                await messages_of_type(live, "barrier", 1)
            return await _recovered_both_ways(relay, anchor, websockets)
        finally:
            blink.close()

    for answer, numbers in asyncio.run(scenario()):
        assert answer["recovered"] is False or 2 in numbers, answer


def test_a_message_its_origin_had_no_room_to_keep_is_never_said_recovered(nodes):
    """The origin of a message had no room left for its history and sent it
    on with no position: a client of the other node whose last message of the
    topic came before it is told it was not recovered, never that it missed
    nothing."""
    websockets = pytest.importorskip("websockets")
    origin = nodes(recovery=True, recovery_memory_budget=1)
    relay = nodes(recovery=True)
    origin.join()
    relay.join(origin)

    async def scenario():
        await until(lambda: origin.peers == 1 and relay.peers == 1)
        async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse") as live:
            await subscribe_next_client(relay, ["news"])
            await asyncio.sleep(0.3)  # the SUB reaches the origin
            relay.server.broadcast_local("news", json.dumps({"t": "news", "p": {"n": 1}}))
            (anchor,) = await messages_of_type(live, "news", 1)
            # The origin's budget is used up: what it publishes has no position.
            origin.server.broadcast_local("other", json.dumps({"t": "other", "p": {}}))
            origin.server.broadcast("news", json.dumps({"t": "news", "p": {"n": 2}}))
            (unkept,) = await messages_of_type(live, "news", 1)
        answers = []
        # As a client names its last message: a scalar position, and streams.
        for streams in ({}, {"streams": {anchor["e"]: anchor["o"]}}):
            async with websockets.connect(f"ws://127.0.0.1:{relay.port}/wse"):
                conn_id = await next_connect(relay)
                past = {"epoch": anchor["e"], "offset": anchor["o"], **streams}
                answer = relay.server.subscribe_with_recovery(
                    conn_id, ["news"], recover=True, positions={"news": past}
                )
                answers.append(answer["topics"]["news"])
        return anchor, unkept, answers

    anchor, unkept, answers = asyncio.run(scenario())
    assert anchor["o"] == 0
    assert unkept["p"]["n"] == 2 and "o" not in unkept
    for answer in answers:
        assert answer["recovered"] is False, answer


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
