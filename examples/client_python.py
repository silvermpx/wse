"""Python client for WSE.

Connects to a WSE standalone server,
subscribes to topics, and prints incoming events.

    pip install wse-client

    # Connect to standalone server
    python examples/client_python.py --url ws://localhost:5007/wse --token <JWT>

Ctrl+C closes the client, which prints what had come before it ends; a
second Ctrl+C stops at once.
"""

import argparse
import asyncio
import signal

from wse_client import connect


async def listen(client) -> None:
    async for event in client:
        print(f"[{event.type}] {event.payload}")


async def main(url: str, token: str, topics: list[str]):
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = (signal.SIGINT, signal.SIGTERM)
    for sig in signals:
        loop.add_signal_handler(sig, stop.set)

    try:
        async with connect(url, token=token) as client:
            await client.subscribe(topics)
            print(f"Connected to {url}")
            print(f"Subscribed to: {topics}")
            print("Listening for events... (Ctrl+C to stop)\n")

            # Whichever comes first: a signal, or the stream's end.
            reading = asyncio.create_task(listen(client))
            stopping = asyncio.create_task(stop.wait())
            await asyncio.wait({reading, stopping}, return_when=asyncio.FIRST_COMPLETED)
            stopping.cancel()
            # The next signal stops at once.
            for sig in signals:
                loop.remove_signal_handler(sig)
            # The stream ends after what it holds.
            await client.disconnect()
            await reading
    finally:
        for sig in signals:
            loop.remove_signal_handler(sig)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WSE Python client")
    parser.add_argument("--url", default="ws://localhost:5007/wse")
    parser.add_argument("--token", required=True, help="JWT token from server output")
    parser.add_argument(
        "--topics",
        default="prices,notifications",
        help="Comma-separated topics (default: prices,notifications)",
    )
    args = parser.parse_args()

    topics = [t.strip() for t in args.topics.split(",")]
    asyncio.run(main(args.url, args.token, topics))
