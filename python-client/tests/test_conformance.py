"""The Python client's driver for the conformance server
(crates/wse-conformance): it runs each case it can, step by step, and records
what its client saw and did; the server judges. Runs when WSE_CONFORMANCE
holds the server's control URL::

    cargo run -p wse-conformance -- serve        # prints the control URL
    WSE_CONFORMANCE=ws://127.0.0.1:P/control pytest python-client/tests/test_conformance.py
"""

from __future__ import annotations

import asyncio
import json
import os
import platform
import sys
import time
from typing import Any

import pytest
import websockets

from wse_client import (
    AsyncWSEClient,
    DeliveryConfig,
    ReconnectConfig,
    SendResult,
    WSEEvent,
    WSETimeoutError,
    generate_signing_key,
)
from wse_client.signing import public_key_base64

CONTROL = os.environ.get("WSE_CONFORMANCE")
if os.environ.get("WSE_CONFORMANCE_REQUIRED") == "1" and not CONTROL:
    raise RuntimeError("WSE_CONFORMANCE_REQUIRED is set, but WSE_CONFORMANCE is not")

#: What the Python client has, by the catalog's names.
FEATURES = ["outbox", "start", "live", "msgpack", "e2e", "signing", "recover_or_resync"]


class Control:
    """The control channel: requests and their answers, by id."""

    def __init__(self, ws: Any) -> None:
        self.ws = ws
        self.next = 0

    async def call(self, op: str, **body: Any) -> dict[str, Any]:
        self.next += 1
        await self.ws.send(json.dumps({"id": self.next, "op": op, **body}))
        while True:
            answer = json.loads(await self.ws.recv())
            if answer.get("id") == self.next:
                if not answer.get("ok"):
                    raise RuntimeError(f"{op}: {answer.get('error')}")
                return answer


class Run:
    """One case: the client, what it saw, and the steps' doings."""

    def __init__(self, begun: dict[str, Any], control: Control) -> None:
        self.begun = begun
        self.control = control
        self.trace: list[dict[str, Any]] = []
        self.client: AsyncWSEClient | None = None
        self.ready = False
        self.rr = False
        self.published: dict[str, list[int]] = {}
        #: Resync rounds waiting for a step to say the application resynced.
        self.resyncs: list[tuple[int, list[str]]] = []
        self.key: Any = None
        self.bound = False
        self.began = time.monotonic()

    def record(self, event: dict[str, Any]) -> None:
        event["t"] = int((time.monotonic() - self.began) * 1000)
        self.trace.append(event)

    async def go(self) -> None:
        for at, step in enumerate(self.begun["steps"]):
            try:
                if "server" in step:
                    self.record({"ev": "step", "server": step["server"]})
                    done = await self.control.call("act", step=step)
                    if step["server"] == "publish":
                        self.published.setdefault(done["topic"], []).extend(done["n"])
                    continue
                if not await self.client_step(step):
                    self.record({"ev": "timeout", "step": at, "waiting": step})
                    break
            except Exception as exc:  # noqa: BLE001 - the judge hears every failure
                self.record({"ev": "error", "message": f"step {at}: {exc!r}"})
                break
        await self.close()

    async def client_step(self, step: dict[str, Any]) -> bool:
        """A step of the client's: False when what it waits for did not come."""
        kind = step["client"]
        if kind == "connect":
            await self.connect(step)
        elif kind == "wait":
            deadline = time.monotonic() + step.get("timeout_ms", 10_000) / 1000
            return await self.until(step, deadline)
        elif kind == "send":
            await self.send(step)
        elif kind == "pause":
            await self.need().pause("test")
        elif kind == "resume":
            await self.need().resume("test")
        elif kind == "resynced":
            topics = step["topics"]
            self.record({"ev": "resynced", "topics": topics})
            waiting, self.resyncs = self.resyncs, []
            for round_, of in waiting:
                if any(t in topics for t in of):
                    self.need().resynced(round_)
                else:
                    self.resyncs.append((round_, of))
        elif kind == "sleep":
            until = time.monotonic() + step.get("ms", 0) / 1000
            while time.monotonic() < until:
                await self.pump(until)
        elif kind == "close":
            await self.close()
        else:
            raise ValueError(f"no such client step: {kind}")
        return True

    def need(self) -> AsyncWSEClient:
        if self.client is None:
            raise RuntimeError("no client")
        return self.client

    async def connect(self, step: dict[str, Any]) -> None:
        kwargs: dict[str, Any] = {
            "token": self.begun["tokens"][step.get("user", "ann")],
            "topics": step.get("topics") or None,
            "reconnect": ReconnectConfig(base_delay=0.05, max_delay=0.3, jitter=False),
            "wire_format": "msgpack" if step.get("format") == "msgpack" else "json",
            # The cases send in bursts; the client's own limit is not judged.
            "rate_limit": None,
        }
        if step.get("e2e"):
            kwargs["encryption"] = True
            kwargs["e2e_server_keys"] = [self.begun["identity_key"]]
        if step.get("signing"):
            self.key = generate_signing_key()
            kwargs["signing_key"] = self.key
            kwargs["server_signing_keys"] = [self.begun["signing_key"]]
            # The application binds the key (bind_key): the token names none.
            kwargs["e2e_bind_session"] = False
        self.rr = bool(step.get("recover_or_resync"))
        if self.rr:
            kwargs["delivery"] = DeliveryConfig(consumer="conformance")
        client = AsyncWSEClient(self.begun["url"], **kwargs)
        self.client = client

        def connected(_event: WSEEvent) -> None:
            self.ready = True
            self.record({"ev": "ready"})

        def gone(_event: WSEEvent) -> None:
            self.ready = False

        client.on("connected")(connected)
        client.on("disconnected")(gone)
        client.on("paused")(gone)
        client.on("delivered")(
            lambda e: self.record({"ev": "delivered", "ids": e.payload["ids"]})
        )
        client.on("not_delivered")(
            lambda e: self.record(
                {
                    "ev": "not_delivered",
                    "reason": e.payload["reason"],
                    "ids": e.payload["ids"],
                }
            )
        )
        if step.get("start"):
            await client.start()
        else:
            await client.connect()

    async def send(self, step: dict[str, Any]) -> None:
        client = self.need()
        count = step.get("count", 1)
        for k in range(1, count + 1):
            message_id = step.get("id") or f"{step['type']}-{k}"
            payload = step.get("payload", {"n": k})
            if step.get("signed"):
                if not self.bound:
                    bind = await client.send(
                        "bind_key",
                        {"public_key": public_key_base64(self.key)},
                        id="bind-key",
                    )
                    self.record(
                        {
                            "ev": "sent",
                            "id": "bind-key",
                            "type": "bind_key",
                            "live": False,
                            "ok": bind is not SendResult.FAILED,
                        }
                    )
                    deadline = time.monotonic() + 10
                    if not await self.until(
                        {"for": "message", "type": "key_bound"}, deadline
                    ):
                        raise RuntimeError("the server never bound the key")
                    self.bound = True
                sent = await client.send(
                    step["type"], payload, signed=True, nonce=message_id
                )
                self.record(
                    {
                        "ev": "sent",
                        "id": message_id,
                        "type": step["type"],
                        "signed": True,
                        "live": False,
                        "ok": sent is not SendResult.FAILED,
                    }
                )
                continue
            live = bool(step.get("live"))
            sent = await client.send(step["type"], payload, id=message_id, live=live)
            self.record(
                {
                    "ev": "sent",
                    "id": message_id,
                    "type": step["type"],
                    "live": live,
                    "ok": sent is not SendResult.FAILED,
                }
            )

    async def pump(self, deadline: float) -> None:
        """Takes the next thing the client has for the application, waiting
        until *deadline* at most."""
        client = self.client
        if client is None:
            await asyncio.sleep(max(0.0, min(0.02, deadline - time.monotonic())))
            return
        wait = max(0.001, min(0.02, deadline - time.monotonic()))
        try:
            event = await client.recv(timeout=wait)
        except WSETimeoutError:
            return
        except Exception:  # noqa: BLE001 - the stream ended
            await asyncio.sleep(wait)
            return
        self.take(event)

    def take(self, event: WSEEvent) -> None:
        if event.type == "recovery_gap":
            self.record({"ev": "break", "topic": event.payload["topic"], "kind": "gap"})
        elif event.type == "continuity":
            if event.payload["continuity"] != "continuous":
                self.record(
                    {
                        "ev": "break",
                        "topic": event.payload["topic"],
                        "kind": "continuity",
                        "reason": event.payload.get("reason"),
                    }
                )
        elif event.type == "resync":
            topics = list(event.payload["topics"])
            # Each topic's reason, by topic.
            reasons = dict(event.payload["reasons"])
            cold = all(r == "cold_start" for r in reasons.values())
            self.record(
                {"ev": "resync", "topics": topics, "auto": cold, "reasons": reasons}
            )
            # At a cold start the application has its state at once.
            if cold:
                self.record({"ev": "resynced", "topics": topics, "auto": True})
                self.need().resynced(event.payload["round"])
            else:
                self.resyncs.append((event.payload["round"], topics))
        elif event.category != "WSE":
            payload = event.payload if isinstance(event.payload, dict) else {}
            n = payload.get("n")
            self.record(
                {
                    "ev": "message",
                    "type": event.type,
                    "topic": event.topic,
                    "n": n if isinstance(n, int) else None,
                    "payload": payload,
                }
            )
            # Applied: the checkpoint moves on.
            if self.rr and event.topic is not None and self.client is not None:
                self.client.commit(event)

    async def until(self, step: dict[str, Any], deadline: float) -> bool:
        while True:
            if self.holds(step):
                return True
            if time.monotonic() >= deadline:
                return False
            await self.pump(deadline)

    def holds(self, step: dict[str, Any]) -> bool:
        def messages(pick) -> list[dict[str, Any]]:  # noqa: ANN001
            return [e for e in self.trace if e["ev"] == "message" and pick(e)]

        topic = step.get("topic")
        what = step["for"]
        if what == "ready":
            return self.ready
        if what == "not_ready":
            return not self.ready
        if what == "messages":
            return len(messages(lambda e: e["topic"] == topic)) >= step["count"]
        if what == "message":
            return bool(messages(lambda e: e["type"] == step["type"]))
        if what == "published":
            wanted = self.published.get(topic, [])[-step["last"] :]
            got = {e["n"] for e in messages(lambda e: e["topic"] == topic)}
            return len(wanted) == step["last"] and all(n in got for n in wanted)
        if what == "break":
            return any(
                (e["ev"] == "break" and e["topic"] == topic)
                or (e["ev"] == "resync" and topic in e["topics"])
                for e in self.trace
            )
        if what == "resync":
            return any(
                e["ev"] == "resync" and not e.get("auto") and topic in e["topics"]
                for e in self.trace
            )
        if what == "delivered":
            delivered = {
                i for e in self.trace if e["ev"] == "delivered" for i in e["ids"]
            }
            if step.get("all"):
                ids = [
                    e["id"]
                    for e in self.trace
                    if e["ev"] == "sent"
                    and e["ok"]
                    and not e["live"]
                    and not e.get("signed")
                ]
            else:
                ids = step["ids"]
            return all(i in delivered for i in ids)
        raise ValueError(f"no such wait: {what}")

    async def close(self) -> None:
        client, self.client = self.client, None
        self.ready = False
        if client is not None:
            await client.disconnect()


@pytest.mark.skipif(not CONTROL, reason="WSE_CONFORMANCE names no conformance server")
async def test_the_python_client_passes_every_case_it_can():
    async with websockets.connect(CONTROL) as ws:
        control = Control(ws)
        hello = await control.call(
            "hello",
            agent="python",
            version="3.2.0",
            runtime=f"python {platform.python_version()}",
            os=sys.platform,
            arch=platform.machine(),
            features=FEATURES,
        )
        only = os.environ.get("WSE_CONFORMANCE_CASES")
        failed = []
        for case in hello["cases"]:
            name = case["case"]
            if only and name not in only.split(","):
                continue
            begun = await control.call("begin", case=name)
            run = Run(begun, control)
            await run.go()
            for at in range(0, len(run.trace), 500):
                await control.call("trace", events=run.trace[at : at + 500])
            judged = await control.call("end")
            if os.environ.get("WSE_CONFORMANCE_TRACE") == "1":
                print(name, json.dumps(run.trace))
            if judged["verdict"] != "pass":
                failed.append(f"{name}: {judged['failures']}")
        await control.call("bye")
    assert failed == []
