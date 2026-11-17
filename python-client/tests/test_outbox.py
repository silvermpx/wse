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

    def test_one_that_requires_topics_waits_in_its_place_each_time_it_goes(self):
        outbox = Outbox(LIMITS)
        outbox.add("a", text("a"), None, 0, requires=("prices",))
        add(outbox, "b")
        answering(outbox)
        assert outbox.due(0, Outcomes(), lambda topics: False) == []
        assert numbers(outbox.due(0, Outcomes(), lambda topics: True)) == [1, 2]
        # Lost before its answer: on the next connection it waits again.
        outbox.disconnected()
        answering(outbox)
        assert outbox.due(0, Outcomes(), lambda topics: False) == []
        assert numbers(outbox.due(0, Outcomes(), lambda topics: True)) == [1, 2]

    def test_another_users_messages_go(self):
        outbox = Outbox(LIMITS)
        add(outbox, "before any")
        outbox.connected("ann", 0)
        outbox.disconnected()
        add(outbox, "anns")
        outcomes = outbox.connected("bob", 0)
        assert outcomes.not_delivered == {"user_changed": ["anns"]}
        assert outbox.size == 1

    def test_a_message_never_goes_again_as_another_user_than_it_was_written_as(self):
        outbox = Outbox(LIMITS)
        add(outbox, "before any")
        answering(outbox)
        assert numbers(outbox.due(0, Outcomes())) == [1]
        # Lost before its answer; the next connection is bob's.
        outbox.disconnected()
        outcomes = outbox.connected("bob", 0)
        assert outcomes.not_delivered == {"user_changed": ["before any"]}
        outbox.server_answers(True)
        outbox.answered(0, [], "n1")
        assert outbox.due(0, Outcomes()) == []

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


# -- The file ------------------------------------------------------------------

import os  # noqa: E402
import pathlib  # noqa: E402
import sys  # noqa: E402
import time  # noqa: E402

import pytest  # noqa: E402

from wse_client import outbox as outbox_module  # noqa: E402

VECTOR = json.loads(
    (
        pathlib.Path(__file__).resolve().parents[2]
        / "crates/wse-client/tests/vectors/outbox.json"
    ).read_text(encoding="utf-8")
)


class TestTheFileFormat:
    """The file of the Rust client's, line by line (tests/vectors/outbox.json)."""

    def test_lines_are_written_byte_for_byte_as_the_vector_has_them(self):
        salt = VECTOR["salt"].encode()
        for line in VECTOR["lines"]:
            body = line["json"]
            if line["kind"] == "head":
                written = outbox_module._line(b"", body)
            elif line["kind"] == "add":
                add = json.loads(body)["add"]
                record = outbox_module._record(body, add)
                assert record is not None
                written = outbox_module._line(salt, outbox_module._add_json(record))
            else:
                written = outbox_module._line(
                    salt, outbox_module._dumps(json.loads(body))
                )
            assert written == (line["line"] + "\n").encode()

    def test_the_file_reads_to_the_state_the_vector_has(self):
        data = "".join(line["line"] + "\n" for line in VECTOR["lines"]).encode()
        read = outbox_module._read(data)
        state = VECTOR["state"]
        assert read is not None
        assert (read.producer, read.node, read.written, read.next, read.user) == (
            state["producer"],
            state["node"],
            state["written"],
            state["next"],
            state["user"],
        )
        assert [
            (
                r.s,
                r.id,
                round(r.at * 1000),
                round(r.until * 1000),
                r.owned,
                r.requires,
                r.text,
            )
            for r in read.records
        ] == [
            (
                r["s"],
                r["id"],
                r["at"],
                r["until"],
                r["owned"],
                tuple(r["requires"]) if "requires" in r else None,
                r["text"],
            )
            for r in state["records"]
        ]

    @pytest.mark.parametrize("damaged", VECTOR["damaged"], ids=lambda d: d["why"])
    def test_a_damaged_file_is_read_up_to_where_it_checks(self, damaged):
        read = outbox_module._read(damaged["file"].encode())
        if damaged["records"] is None:
            assert read is None
            return
        assert read is not None
        assert [r.id for r in read.records] == damaged["records"]
        assert read.user == damaged["user"]


def until(test, within: float = 5.0) -> None:
    """Waits until *test* holds, or fails after *within* seconds."""
    deadline = time.monotonic() + within
    while not test():
        assert time.monotonic() < deadline, "not within the time"
        time.sleep(0.01)


class TestTheOutboxInAFile:
    @staticmethod
    def offline(tmp_path) -> Offline:
        return Offline(path=str(tmp_path / "queue"))

    def test_what_is_kept_outlives_the_client_until_the_server_took_it(self, tmp_path):
        offline = self.offline(tmp_path)
        first = Outbox(offline)
        answering(first)
        for message_id in ("a", "b", "c"):
            add(first, message_id)
        first.due(0, Outcomes())
        first.acked(1)
        first.close_file()

        again = Outbox(offline)
        assert again.producer == first.producer
        assert again.size == 2
        assert again.hello() == {
            "producer": first.producer,
            "node": "n1",
            "written": 3,
            "from": 2,
        }
        answering(again, 1)
        out = again.due(0, Outcomes())
        assert numbers(out) == [2, 3]
        assert json.loads(out[0][1])["id"] == "b"
        assert add(again, "d") == 4
        again.close_file()

    def test_a_number_is_never_used_twice(self, tmp_path):
        offline = self.offline(tmp_path)
        first = Outbox(offline)
        add(first, "a")
        add(first, "b")
        first.clear()
        first.close_file()
        again = Outbox(offline)
        assert again.size == 0
        assert add(again, "c") == 3
        again.close_file()

    def test_a_tail_that_does_not_check_is_not_read(self, tmp_path):
        offline = self.offline(tmp_path)
        first = Outbox(offline)
        add(first, "a")
        add(first, "b")
        first.close_file()
        path = tmp_path / "queue"
        data = path.read_bytes()
        path.write_bytes(data[:-5] + b"\xff\xfe\x00\n")
        again = Outbox(offline)
        assert again.size == 1
        again.close_file()

    def test_a_file_another_client_has_is_said_and_kept_in_memory(self, tmp_path):
        offline = self.offline(tmp_path)
        first = Outbox(offline)
        said: list[tuple[str, str]] = []
        second = Outbox(offline, report=lambda op, why: said.append((op, why)))
        assert len(said) == 1 and said[0][0] == "open"
        assert "another client's" in said[0][1]
        add(second, "a")
        assert second.size == 1
        first.close_file()

    def test_what_a_message_requires_outlives_the_client(self, tmp_path):
        offline = self.offline(tmp_path)
        first = Outbox(offline)
        first.add("a", text("a"), None, 0, requires=("prices", "news"))
        first.close_file()
        again = Outbox(offline)
        answering(again)
        assert (
            again.due(0, Outcomes(), lambda topics: topics != ("prices", "news")) == []
        )
        assert numbers(again.due(0, Outcomes(), lambda topics: True)) == [1]
        again.close_file()

    def test_the_file_is_written_anew_once_what_went_outweighs_what_is_kept(
        self, tmp_path
    ):
        offline = self.offline(tmp_path)
        outbox = Outbox(offline)
        answering(outbox)
        for i in range(300):
            s = add(outbox, f"m{i}")
            outbox.due(0, Outcomes())
            outbox.acked(s)
        outbox.close_file()
        lines = (tmp_path / "queue").read_text().splitlines()
        assert len(lines) < 80
        again = Outbox(offline)
        assert again.size == 0
        assert add(again, "next") == 301
        again.close_file()

    @pytest.mark.skipif(
        sys.platform == "win32" or os.geteuid() == 0,
        reason="a directory that takes no file",
    )
    def test_a_rewrite_owed_within_the_second_comes_when_woken(self, tmp_path):
        offline = self.offline(tmp_path)
        said: list[str] = []
        outbox = Outbox(offline, report=lambda op, why: said.append(op))
        woken: list[float] = []
        outbox.wake = woken.append
        answering(outbox)
        tmp_path.chmod(0o500)
        try:
            # Enough went to write the file anew: that fails (the folder
            # takes no file), and so does the rewrite at once after it.
            for i in range(100):
                s = add(outbox, f"m{i}")
                outbox.due(0, Outcomes())
                outbox.acked(s)
            until(lambda: said == ["write"])
            add(outbox, "a")
            outbox.due(0, Outcomes())
            time.sleep(0.2)
        finally:
            tmp_path.chmod(0o700)
        # Within the second after that rewrite: none yet, a wake asked for.
        woken.clear()
        add(outbox, "b")
        outbox.due(0, Outcomes())
        assert len(woken) == 1 and 0 < woken[0] <= 1.0
        time.sleep(woken[0])
        outbox.flush()
        # The file holds both, with no close and no further change.
        until(lambda: '"b"' in (tmp_path / "queue").read_text())
        assert '"a"' in (tmp_path / "queue").read_text()
        outbox.close_file()

    @pytest.mark.skipif(
        sys.platform == "win32" or os.geteuid() == 0,
        reason="a directory that takes no file",
    )
    def test_a_file_that_cannot_be_written_says_so_and_heals(self, tmp_path):
        offline = self.offline(tmp_path)
        said: list[tuple[str, str]] = []
        outbox = Outbox(offline, report=lambda op, why: said.append((op, why)))
        answering(outbox)
        tmp_path.chmod(0o500)
        try:
            for i in range(100):
                s = add(outbox, f"m{i}")
                outbox.due(0, Outcomes())
                outbox.acked(s)
            add(outbox, "kept")
            outbox.due(0, Outcomes())
            deadline = time.monotonic() + 5
            while not said and time.monotonic() < deadline:
                time.sleep(0.01)
            assert [op for op, _ in said] == ["write"]
        finally:
            tmp_path.chmod(0o700)
        outbox.disconnected()
        add(outbox, "after")
        outbox.close_file()
        again = Outbox(offline)
        assert again.size == 2
        again.close_file()
