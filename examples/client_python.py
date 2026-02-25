"""Python client for WSE.

Connects to a standalone or router-mode WSE server,
subscribes to topics, and prints incoming events.

    pip install wse-client

    # Connect to standalone server
    python examples/client_python.py --url ws://localhost:5007/wse --token <JWT>

    # Connect to router-mode server
    python examples/client_python.py --url ws://localhost:8000/wse --token <JWT>
"""

import argparse
import asyncio
import signal

from wse_client import connect


async def main(url: str, token: str, topics: list[str]):
    stop = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with connect(url, token=token) as client:
        await client.subscribe(topics)
        print(f"Connected to {url}")
        print(f"Subscribed to: {topics}")
        print("Listening for events... (Ctrl+C to stop)\n")

        async for event in client:
            print(f"[{event.type}] {event.payload}")

            if stop.is_set():
                break


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
