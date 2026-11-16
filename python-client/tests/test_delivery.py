"""The recover-or-resync delivery, as the TypeScript and Rust clients have it:
a topic not continuous holds its messages until the application resynced it,
the checkpoint moves through what was applied without a hole, recovery
starts from it, and a connection of another user starts over."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

from wse_client import Continuity, DeliveryConfig, Subscription
from wse_client.client import AsyncWSEClient
from wse_client.types import ConnectionState, WSEEvent

EPOCH = "0000000a"
URL = "ws://localhost:5006/wse"


def make_client(
    delivery: DeliveryConfig | None = DeliveryConfig(consumer="board"),
) -> AsyncWSEClient:
    c = AsyncWSEClient(URL, recovery_timeout=0.2, delivery=delivery)
    c._event_queue = asyncio.Queue(maxsize=c._queue_size)
    c._server_ready_event = asyncio.Event()
    c._connection = MagicMock()
    c._connection.is_connected = True
    c._connection.state = ConnectionState.CONNECTED
    c._connection.send = AsyncMock(return_value=True)
    c._server_recovery_enabled = True
    return c


def stamped(offset: int, *, topic: str = "prices", epoch: str = EPOCH) -> str:
    return json.dumps(
        {
            "tp": topic,
            "e": epoch,
            "o": offset,
            "c": "U",
            "t": "price",
            "p": {"n": offset},
        }
    )


def server_ready(user: str) -> str:
    return json.dumps(
        {"c": "WSE", "t": "server_ready", "p": {"details": {"user_id": user}}}
    )


def taken(*topics: str) -> str:
    return json.dumps(
        {"c": "WSE", "t": "subscription_update", "p": {"success_topics": list(topics)}}
    )


def drain(client: AsyncWSEClient) -> list[WSEEvent]:
    events = []
    while not client._event_queue.empty():
        events.append(client._event_queue.get_nowait())
    return events


def numbers(events: list[WSEEvent]) -> list[int]:
    return [e.offset for e in events if e.type == "price" and e.offset is not None]


def resync_of(events: list[WSEEvent]) -> dict:
    for e in events:
        if e.type == "resync":
            return e.payload
    raise AssertionError(f"a resync round: {[e.type for e in events]}")


def sent_recovery(client: AsyncWSEClient) -> list[dict]:
    out = []
    for call in client._connection.send.call_args_list:
        msg = json.loads(call[0][0])
        if msg["t"] == "subscription_update" and msg["p"].get("recover"):
            out.append(msg["p"]["recovery"])
    return out


async def connected(client: AsyncWSEClient, user: str = "ann") -> dict:
    """A connection of *user* that took prices: its resync round."""
    client._on_raw_message(server_ready(user))
    await client.subscribe(["prices"])
    client._on_raw_message(taken("prices"))
    return resync_of(drain(client))


class TestRecoverOrResync:
    async def test_a_cold_start_holds_the_topic_until_the_application_resynced_it(self):
        c = make_client()
        round_ = await connected(c)
        assert round_["topics"] == ["prices"]
        assert round_["reasons"] == {"prices": "cold_start"}
        assert round_["committed"] == {}
        state = c.topic_states["prices"]
        assert (state.subscription, state.continuity) == (
            Subscription.ACTIVE,
            Continuity.RESYNCING,
        )
        for o in (1, 2):
            c._on_raw_message(stamped(o))
        assert numbers(drain(c)) == [], "held while the application loads its state"
        c.resynced(round_["round"])
        events = drain(c)
        assert numbers(events) == [1, 2]
        assert any(
            e.type == "continuity" and e.payload["continuity"] == "continuous"
            for e in events
        )
        c._on_raw_message(stamped(3))
        assert numbers(drain(c)) == [3]

    async def test_the_checkpoint_moves_through_what_was_applied_without_a_hole(self):
        c = make_client()
        c.resynced((await connected(c, "hal"))["round"])
        for o in (1, 2, 3, 4):
            c._on_raw_message(stamped(o))
        m1, m2, m3, _m4 = [e for e in drain(c) if e.type == "price"]
        # A stream starts just before the first message handed on.
        assert json.loads(c.checkpoint())["topics"] == {"prices": {EPOCH: 0}}
        c.commit(m1)
        c.commit(m3)
        assert json.loads(c.checkpoint())["topics"] == {"prices": {EPOCH: 1}}
        c.commit(m2)
        saved = json.loads(c.checkpoint())
        assert saved == {
            "v": 1,
            "realm": URL,
            "user": "hal",
            "consumer": "board",
            "schema": None,
            "topics": {"prices": {EPOCH: 3}},
        }

    async def test_a_reconnect_resyncs_and_recovers_from_what_was_committed(self):
        c = make_client()
        c.resynced((await connected(c, "hal"))["round"])
        for o in (1, 2, 3):
            c._on_raw_message(stamped(o))
        for e in [e for e in drain(c) if e.type == "price"][:2]:
            c.commit(e)
        # The connection goes and comes back for the same user.
        c._on_transport_close()
        assert c.topic_states["prices"].reason == "reconnected"
        c._on_raw_message(server_ready("hal"))
        c._resubscribe_pending = {"prices"}
        await c._resubscribe()
        assert sent_recovery(c)[-1]["prices"]["offset"] == 2, "from what was committed"
        c._on_raw_message(taken("prices"))
        round_ = resync_of(drain(c))
        assert round_["reasons"] == {"prices": "reconnected"}
        assert round_["committed"] == {"prices": {EPOCH: 2}}
        # 3 came but was not applied: replayed, it comes again.
        c._on_raw_message(stamped(3))
        c.resynced(round_["round"])
        assert numbers(drain(c)) == [3]

    async def test_a_message_of_the_last_user_never_moves_the_next_ones_checkpoint(
        self,
    ):
        c = make_client()
        c.resynced((await connected(c, "ann"))["round"])
        c._on_raw_message(stamped(1))
        anns = [e for e in drain(c) if e.type == "price"][0]
        c.commit(anns)
        assert json.loads(c.checkpoint())["user"] == "ann"
        c._on_transport_close()
        c._on_raw_message(server_ready("bob"))
        c.commit(anns)
        saved = json.loads(c.checkpoint())
        assert (saved["user"], saved["topics"]) == ("bob", {})

    async def test_a_checkpoint_of_another_consumer_is_refused_aloud(self):
        saved = json.dumps(
            {
                "v": 1,
                "realm": URL,
                "user": "kim",
                "consumer": "other",
                "schema": None,
                "topics": {"prices": {EPOCH: 9}},
            }
        )
        c = make_client(DeliveryConfig(consumer="board", checkpoint=saved))
        c._on_raw_message(server_ready("kim"))
        await c.subscribe(["prices"])
        events = drain(c)
        assert [e.payload for e in events if e.type == "checkpoint_rejected"] == [
            {"reason": "consumer"}
        ]
        assert "prices" not in (sent_recovery(c) or [{}])[-1], "nothing of it is taken"

    async def test_a_checkpoint_of_this_consumer_is_where_recovery_starts(self):
        saved = json.dumps(
            {
                "v": 1,
                "realm": URL,
                "user": "kim",
                "consumer": "board",
                "schema": None,
                "topics": {"prices": {EPOCH: 9}},
            }
        )
        c = make_client(DeliveryConfig(consumer="board", checkpoint=saved))
        c._on_raw_message(server_ready("kim"))
        await c.subscribe(["prices"])
        assert sent_recovery(c)[-1]["prices"]["offset"] == 9

    async def test_messages_held_past_the_limit_start_the_resync_over(self):
        c = make_client(DeliveryConfig(consumer="board", max_held=2))
        round_ = await connected(c)
        for o in (1, 2, 3):
            c._on_raw_message(stamped(o))
        c.resynced(round_["round"])
        events = drain(c)
        assert numbers(events) == []
        assert resync_of(events)["reasons"] == {"prices": "overflow"}

    async def test_a_break_while_it_resyncs_asks_for_the_topic_again(self):
        c = make_client()
        round_ = await connected(c)
        c._report_gap("prices", EPOCH, 3, 4, "timeout")
        c.resynced(round_["round"])
        assert resync_of(drain(c))["reasons"] == {"prices": "timeout"}

    async def test_a_handler_that_resyncs_hears_what_was_held_once_it_returned(self):
        c = make_client()
        heard: list[str] = []

        def on_resync(event: WSEEvent) -> None:
            heard.append("resync")
            c.resynced(event.payload["round"])
            heard.append("returned")

        c.on("resync")(on_resync)
        c.on("price")(lambda e: heard.append(f"price {e.offset}"))
        c._on_raw_message(server_ready("ann"))
        await c.subscribe(["prices"])
        c._on_raw_message(stamped(1))
        c._on_raw_message(taken("prices"))
        assert heard == ["resync", "returned", "price 1"]


class TestBestEffort:
    async def test_it_holds_nothing_and_lets_the_app_say_it_resynced(self):
        c = make_client(None)
        c._on_raw_message(server_ready("lee"))
        await c.subscribe(["prices"])
        c._on_raw_message(taken("prices"))
        assert c.topic_states["prices"].continuity is Continuity.CONTINUOUS
        c._on_raw_message(stamped(1))
        assert numbers(drain(c)) == [1]
        c._report_gap("prices", EPOCH, 2, 3, "timeout")
        assert c.topic_states["prices"].continuity is Continuity.RESYNC_REQUIRED
        c.resync(["prices"])
        assert c.topic_states["prices"].continuity is Continuity.CONTINUOUS

    async def test_another_users_connection_does_not_recover_from_the_last_ones(self):
        c = make_client(None)
        c._on_raw_message(server_ready("ann"))
        await c.subscribe(["prices"])
        for o in (1, 2, 3):
            c._on_raw_message(stamped(o))
        drain(c)
        c._on_transport_close()
        c._on_raw_message(server_ready("bob"))
        c._resubscribe_pending = {"prices"}
        await c._resubscribe()
        assert sent_recovery(c) == [], "bob's topics start over: nothing of ann's"
