"""The server examples, as an application and the official client use them."""

import asyncio
import importlib.util
import pathlib
import threading

import pytest

EXAMPLES = pathlib.Path(__file__).resolve().parents[1] / "examples"


def example(name: str):
    spec = importlib.util.spec_from_file_location(name, EXAMPLES / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def next_price(client, timeout: float):
    async with asyncio.timeout(timeout):
        async for event in client:
            if event.type == "price_update":
                return event


def test_the_broadcast_example_takes_the_official_clients_subscriptions(server_port):
    wse_client = pytest.importorskip("wse_client")
    broadcast = example("standalone_broadcast")
    server = broadcast.make_server("127.0.0.1", server_port)
    server.start()
    stop = threading.Event()
    threads = [
        threading.Thread(target=broadcast.event_loop, args=(server, stop), daemon=True),
        threading.Thread(target=broadcast.publisher, args=(server, stop), daemon=True),
    ]
    for thread in threads:
        thread.start()

    async def scenario():
        url = f"ws://127.0.0.1:{server_port}/wse"
        async with wse_client.connect(url, token=broadcast.make_token("ann")) as client:
            # Subscribed by the example on connect: its ticks come.
            assert await next_price(client, 5) is not None
            assert await client.unsubscribe(["prices"])
            # What was on its way may still come; then nothing more.
            await asyncio.sleep(0.3)
            with pytest.raises(TimeoutError):
                await next_price(client, 0.5)

    try:
        asyncio.run(scenario())
    finally:
        stop.set()
        for thread in threads:
            thread.join(timeout=5)
        server.stop()
