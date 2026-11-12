"""Offset-gap recovery: no topic blackout, anchor = last delivered."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from wse_client.client import RECOVERY_GAP_EVENT, AsyncWSEClient
from wse_client.types import ConnectionState, WSEEvent

EPOCH = "0000000a"


def make_client(*, recovery: bool = True, timeout: float = 0.2) -> AsyncWSEClient:
    c = AsyncWSEClient("ws://localhost:5006/wse", recovery_timeout=timeout)
    c._event_queue = asyncio.Queue(maxsize=c._queue_size)
    c._server_ready_event = asyncio.Event()
    c._connection = MagicMock()
    c._connection.is_connected = True
    c._connection.state = ConnectionState.CONNECTED
    c._connection.send = AsyncMock(return_value=True)
    c._server_recovery_enabled = recovery
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


def reply(topic: str, *, offset: int, recovered: bool, epoch: str = EPOCH) -> str:
    return json.dumps(
        {
            "c": "WSE",
            "t": "subscription_update",
            "p": {
                "success_topics": [topic],
                "recovery": {
                    topic: {"epoch": epoch, "offset": offset, "recovered": recovered}
                },
            },
        }
    )


def drain(client: AsyncWSEClient) -> list[WSEEvent]:
    events = []
    while not client._event_queue.empty():
        events.append(client._event_queue.get_nowait())
    return events


def offsets(events: list[WSEEvent]) -> list[object]:
    return [
        ("gap", e.payload["first_missing"], e.payload["last_missing"])
        if e.type == RECOVERY_GAP_EVENT
        else e.offset
        for e in events
    ]


def recovery_requests(client: AsyncWSEClient) -> list[dict]:
    out = []
    for call in client._connection.send.call_args_list:
        msg = json.loads(call[0][0])
        if msg["t"] == "subscription_update" and msg["p"].get("recover"):
            out.append(msg["p"]["recovery"])
    return out


class TestNoBlackout:
    async def test_gap_without_recovery_is_reported_and_delivered(self):
        c = make_client(recovery=False)
        for o in (1, 2, 6, 7):
            c._on_raw_message(stamped(o))
        assert offsets(drain(c)) == [1, 2, ("gap", 3, 5), 6, 7]
        gap = c.get_stats()["recovery"]
        assert gap["gaps"] == 1 and gap["missed"] == 3

    async def test_unanswered_recovery_times_out_and_keeps_delivering(self):
        """The app does not answer recovery requests; the topic used to
        stay dark until a server restart."""
        c = make_client(timeout=0.1)
        c._on_raw_message(stamped(1))
        c._on_raw_message(stamped(4))  # gap: 2..3 missing
        c._on_raw_message(stamped(5))
        await asyncio.sleep(0)
        assert recovery_requests(c) == [{"prices": {"epoch": EPOCH, "offset": 1}}]
        assert offsets(drain(c)) == [1]  # 4 and 5 held while recovering
        await asyncio.sleep(0.2)
        assert offsets(drain(c)) == [("gap", 2, 3), 4, 5]
        c._on_raw_message(stamped(6))
        assert offsets(drain(c)) == [6]  # no blackout afterwards
        assert c.recovery_state["prices"] == (EPOCH, 6)

    async def test_recovery_replay_fills_the_hole_in_order(self):
        c = make_client()
        c._on_raw_message(stamped(1))
        c._on_raw_message(stamped(4))
        await asyncio.sleep(0)
        for o in (2, 3, 4):  # replay from offset 1 (4 again: a duplicate)
            c._on_raw_message(stamped(o))
        c._on_raw_message(reply("prices", offset=4, recovered=True))
        c._on_raw_message(stamped(5))
        assert offsets(drain(c)) == [1, 2, 3, 4, 5]
        assert c.get_stats()["recovery"]["gaps"] == 0
        assert c.get_stats()["recovery"]["recovered"] == 1

    async def test_only_one_request_in_flight_per_topic(self):
        c = make_client()
        c._on_raw_message(stamped(1))
        for o in (4, 5, 6, 7):
            c._on_raw_message(stamped(o))
        await asyncio.sleep(0)
        assert len(recovery_requests(c)) == 1

    async def test_not_recovered_reply_gives_up_at_once(self):
        c = make_client(timeout=5.0)
        c._on_raw_message(stamped(1))
        c._on_raw_message(stamped(4))
        await asyncio.sleep(0)
        c._on_raw_message(reply("prices", offset=4, recovered=False))
        assert offsets(drain(c)) == [1, ("gap", 2, 3), 4]
        assert c._gaps == {}

    async def test_stops_asking_after_unanswered_requests(self, monkeypatch):
        import wse_client.client as client_mod

        monkeypatch.setattr(client_mod, "RECOVERY_MAX_UNANSWERED", 1)
        c = make_client(timeout=0.05)
        c._on_raw_message(stamped(1))
        c._on_raw_message(stamped(3))
        await asyncio.sleep(0.15)  # unanswered: given up after the timeout
        drain(c)
        c._on_raw_message(stamped(5))  # next gap: reported at once
        assert offsets(drain(c)) == [("gap", 4, 4), 5]
        assert len(recovery_requests(c)) == 1

    async def test_buffer_overflow_gives_up(self, monkeypatch):
        import wse_client.client as client_mod

        monkeypatch.setattr(client_mod, "RECOVERY_BUFFER_MAX", 3)
        c = make_client(timeout=5.0)
        c._on_raw_message(stamped(1))
        for o in (3, 4, 5, 6):
            c._on_raw_message(stamped(o))
        assert offsets(drain(c)) == [1, ("gap", 2, 2), 3, 4, 5, 6]

    async def test_epoch_change_while_waiting(self):
        c = make_client(timeout=5.0)
        c._on_raw_message(stamped(1))
        c._on_raw_message(stamped(4))
        c._on_raw_message(stamped(0, epoch="0000000b"))  # server restarted
        assert offsets(drain(c)) == [1, ("gap", 2, 3), 4, 0]
        assert c.recovery_state["prices"] == ("0000000b", 0)

    async def test_disconnect_delivers_held_messages(self):
        c = make_client(timeout=5.0)
        c._connection.destroy = AsyncMock()
        c._on_raw_message(stamped(1))
        c._on_raw_message(stamped(4))
        await c.disconnect()
        events = drain(c)
        assert offsets(events[:-1]) == [1, ("gap", 2, 3), 4]


class TestAnchor:
    async def test_partial_replay_asks_again_from_last_delivered(self):
        """A truncated replay replied recovered:true with the head; the
        client stored the head as its anchor and the topic went dark."""
        c = make_client(timeout=5.0)
        c._on_raw_message(stamped(1))
        c._on_raw_message(stamped(10))
        await asyncio.sleep(0)
        for o in (2, 3, 4):  # replay stopped early (back-pressure)
            c._on_raw_message(stamped(o))
        c._on_raw_message(reply("prices", offset=10, recovered=True))
        await asyncio.sleep(0)
        assert recovery_requests(c) == [
            {"prices": {"epoch": EPOCH, "offset": 1}},
            {"prices": {"epoch": EPOCH, "offset": 4}},  # not the head (10)
        ]
        assert c.recovery_state["prices"] == (EPOCH, 4)
        for o in range(5, 10):
            c._on_raw_message(stamped(o))
        assert offsets(drain(c)) == list(range(1, 11))

    async def test_recovered_reply_never_moves_the_anchor(self):
        c = make_client()
        c._on_raw_message(stamped(5))
        await c.subscribe(["prices"], recover=True)
        c._on_raw_message(reply("prices", offset=400, recovered=True))
        assert c.recovery_state["prices"] == (EPOCH, 5)

    async def test_plain_subscribe_reply_never_moves_the_anchor(self):
        c = make_client()
        c._on_raw_message(stamped(5))
        c._on_raw_message(reply("prices", offset=2, recovered=False))
        c._on_raw_message(stamped(6))
        assert offsets(drain(c)) == [5, 6]

    async def test_unrecoverable_history_is_reported_on_next_message(self):
        c = make_client(timeout=5.0)
        c._on_raw_message(stamped(5))
        await c.subscribe(["prices"], recover=True)  # e.g. after a reconnect
        c._on_raw_message(reply("prices", offset=100, recovered=False))
        c._on_raw_message(stamped(101))
        assert offsets(drain(c)) == [5, ("gap", 6, 100), 101]
        assert len(recovery_requests(c)) == 1  # no pointless second request

    async def test_epoch_change_in_reply_is_reported(self):
        c = make_client()
        c._on_raw_message(stamped(5))
        await c.subscribe(["prices"], recover=True)
        c._on_raw_message(reply("prices", offset=3, recovered=False, epoch="0000000b"))
        events = drain(c)
        assert events[-1].type == RECOVERY_GAP_EVENT
        assert events[-1].payload["reason"] == "epoch_changed"
        assert events[-1].payload["missed"] is None


class TestTopicLifecycle:
    async def test_unsubscribe_clears_the_position(self):
        """After unsubscribe + subscribe the first message was a gap."""
        c = make_client(recovery=False)
        await c.subscribe(["prices"])
        c._on_raw_message(stamped(5))
        await c.unsubscribe(["prices"])
        assert "prices" not in c.recovery_state
        await c.subscribe(["prices"])
        c._on_raw_message(stamped(50))
        assert offsets(drain(c)) == [5, 50]
        assert c.get_stats()["recovery"]["gaps"] == 0

    async def test_server_hello_announces_recovery(self):
        c = make_client(recovery=False)
        c._handle_server_hello(
            WSEEvent(type="server_hello", payload={"features": {"recovery": True}})
        )
        assert c.recovery_enabled is True

    @pytest.mark.parametrize("payload", [{"recovery": None}, {"recovery": [1]}])
    async def test_malformed_recovery_reply_is_ignored(self, payload):
        c = make_client()
        c._on_raw_message(
            json.dumps({"c": "WSE", "t": "subscription_update", "p": payload})
        )
        c._on_raw_message(stamped(1))
        assert offsets(drain(c)) == [1]
