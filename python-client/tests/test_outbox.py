"""The outbox: a message waits until the server answers for it, goes again
after a reconnect, and the application hears what became of it."""

from __future__ import annotations

import json

from wse_client.outbox import Offline, Outbox, Outcomes, numbered

LIMITS = Offline(max_messages=100, max_bytes=1 << 20, max_age=3600.0)


def text(message_id: str) -> str:
    return json.dumps({"c": "U", "t": "m", "p": {}, "id": message_id})


def add(
    outbox: Outbox, message_id: str, ttl: float | None = None, now: float = 0
) -> int:
    return outbox.add(message_id, text(message_id), ttl, now)


def numbers(out: list[tuple[int, str]]) -> list[int]:
    return [s for s, _ in out]


def answering(outbox: Outbox, acked: int = 0) -> Outcomes:
    outbox.connected("ann", 0)
    outbox.server_answers(True)
    return outbox.answered(acked, [], "n1")


class TestOutbox:
    def test_a_message_waits_for_the_answer_and_goes_again(self):
        outbox = Outbox(LIMITS)
        add(outbox, "a")
        add(outbox, "b")
        outcomes = Outcomes()
        assert outbox.due(0, outcomes) == []
        outbox.connected("ann", 0)
        outbox.server_answers(True)
        assert outbox.due(0, outcomes) == []
        assert outbox.hello()["from"] == 1
        outbox.answered(0, [], "n1")
        out = outbox.due(0, outcomes)
        assert numbers(out) == [1, 2]
        first = json.loads(out[0][1])
        assert list(first)[0] == "s" and first["s"] == 1 and first["id"] == "a"
        outbox.wrote(2)
        assert outbox.acked(1).delivered == ["a"]
        # Lost before 2 was answered: it goes again on the next connection.
        assert not outbox.disconnected()
        assert outbox.hello() == {
            "producer": outbox.producer,
            "node": "n1",
            "written": 2,
            "from": 2,
        }
        assert not answering(outbox, 1).delivered
        assert numbers(outbox.due(0, outcomes)) == [2]
        add(outbox, "c")
        assert numbers(outbox.due(0, outcomes)) == [3]
        assert outbox.acked(3).delivered == ["b", "c"]
        assert outbox.size == 0
        assert not outcomes

    def test_the_answer_may_come_before_the_server_takes_the_connection(self):
        outbox = Outbox(LIMITS)
        add(outbox, "a")
        outbox.server_answers(True)
        outbox.answered(0, [], "n1")
        assert outbox.due(0, Outcomes()) == []
        outbox.connected("ann", 0)
        assert numbers(outbox.due(0, Outcomes())) == [1]

    def test_a_server_without_answers_takes_what_was_written(self):
        outbox = Outbox(LIMITS)
        add(outbox, "a")
        outbox.connected(None, 0)
        outbox.server_answers(False)
        out = outbox.due(0, Outcomes())
        assert out == [(1, text("a"))]
        assert outbox.wrote(1).delivered == ["a"]
        assert outbox.size == 0
        assert outbox.hello()["written"] == 0

    def test_another_users_messages_go(self):
        outbox = Outbox(LIMITS)
        add(outbox, "before any")
        outbox.connected("ann", 0)
        outbox.disconnected()
        add(outbox, "anns")
        outcomes = outbox.connected("bob", 0)
        assert outcomes.not_delivered == {"user_changed": ["anns"]}
        assert outbox.size == 1

    def test_one_not_written_in_its_time_runs_out_and_one_written_waits(self):
        outbox = Outbox(LIMITS)
        add(outbox, "late", ttl=5, now=0)
        add(outbox, "on time", now=0)
        assert outbox.deadline() == 5
        outcomes = Outcomes()
        outbox.expire(10, outcomes)
        assert outcomes.not_delivered == {"expired": ["late"]}
        answering(outbox)
        add(outbox, "quick", ttl=5, now=10)
        assert numbers(outbox.due(10, Outcomes())) == [2, 3]
        outbox.disconnected()
        answering(outbox)
        later = Outcomes()
        assert numbers(outbox.due(100, later)) == [2, 3]
        assert not later

    def test_a_refusal_is_said_one_heard_with_the_answer_too(self):
        outbox = Outbox(LIMITS)
        answering(outbox)
        for message_id in ("a", "b", "c"):
            add(outbox, message_id)
        outbox.due(0, Outcomes())
        assert outbox.refused(1, 1, "busy").not_delivered == {"busy": ["a"]}
        outbox.disconnected()
        outbox.connected("ann", 0)
        outbox.server_answers(True)
        outcomes = outbox.answered(3, [(2, 2, "rate_limited")], "n1")
        assert outcomes.delivered == ["c"]
        assert outcomes.not_delivered == {"rate_limited": ["b"]}
        assert outbox.size == 0
        assert outbox.hello()["from"] == 4

    def test_a_clear_tells_what_the_server_had_in_hand_by_its_answer(self):
        outbox = Outbox(LIMITS)
        answering(outbox)
        add(outbox, "a")
        add(outbox, "b")
        outbox.due(0, Outcomes())
        outbox.wrote(2)
        add(outbox, "c")
        assert outbox.clear().not_delivered == {"cleared": ["c"]}
        assert outbox.size == 0
        assert outbox.acked(1).delivered == ["a"]
        assert outbox.disconnected().not_delivered == {"cleared": ["b"]}
        answering(outbox)
        assert outbox.due(0, Outcomes()) == []

    def test_it_refuses_past_its_limits(self):
        outbox = Outbox(Offline(max_messages=2, max_bytes=100, max_age=1.0))
        assert outbox.add("a", "x" * 10, None, 0) == 1
        assert outbox.add("b", "x" * 80, None, 0) == 2
        assert outbox.add("c", "x", None, 0) == 0
        answering(outbox)
        outbox.due(0, Outcomes())
        outbox.acked(2)
        assert outbox.add("d", "x" * 101, None, 0) == 0
        assert outbox.add("e", "x" * 100, None, 0) == 3

    def test_it_owes_an_answer_for_what_it_wrote_and_to_its_hello(self):
        outbox = Outbox(LIMITS)
        add(outbox, "a")
        outbox.connected("ann", 0)
        outbox.server_answers(True)
        assert outbox.owed()
        outbox.answered(0, [], "n1")
        outbox.due(0, Outcomes())
        outbox.wrote(1)
        assert outbox.owed()
        outbox.acked(1)
        assert not outbox.owed()

    def test_closing_leaves_what_waits_not_delivered(self):
        outbox = Outbox(LIMITS)
        add(outbox, "a")
        assert outbox.close().not_delivered == {"closed": ["a"]}
        assert outbox.size == 0

    def test_many_acks_stay_cheap_and_in_order(self):
        outbox = Outbox(Offline(max_messages=10_000))
        answering(outbox)
        for i in range(5000):
            add(outbox, f"m{i}")
        assert len(outbox.due(0, Outcomes())) == 5000
        delivered = sum(len(outbox.acked(s).delivered) for s in range(64, 5001, 64))
        delivered += len(outbox.acked(5000).delivered)
        assert delivered == 5000
        add(outbox, "next")
        assert numbers(outbox.due(0, Outcomes())) == [5001]

    def test_the_number_goes_first(self):
        assert numbered(7, '{"t":"x"}') == '{"s":7,"t":"x"}'
        assert numbered(7, "{}") == '{"s":7}'
        assert json.loads(numbered(12, ' { "t": 1}')) == {"s": 12, "t": 1}
