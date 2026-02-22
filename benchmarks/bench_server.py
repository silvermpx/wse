"""Minimal WSE benchmark server.

Starts the Rust WSE server with JWT auth and processes drain events.
No database, no application framework needed — just the raw Rust engine.

Usage:
    python benchmarks/bench_server.py                  # port 5006
    python benchmarks/bench_server.py --port 9000
    python benchmarks/bench_server.py --max-connections 5000
    python benchmarks/bench_server.py --token-only      # print JWT token and exit
"""

import argparse
import asyncio
import signal
import time

from wse_server._wse_accel import RustWSEServer, rust_jwt_encode

# JWT config — matches what the benchmark client will use
JWT_SECRET = b"bench-secret-key-for-testing-only"
JWT_ISSUER = "wse-bench"
JWT_AUDIENCE = "wse-bench"


def generate_token(user_id: str = "bench-user") -> str:
    """Generate a JWT token for benchmark clients."""
    claims = {
        "sub": user_id,
        "iss": JWT_ISSUER,
        "aud": JWT_AUDIENCE,
        "exp": int(time.time()) + 3600,
        "iat": int(time.time()),
    }
    return rust_jwt_encode(claims, JWT_SECRET)


async def run_server(port: int, max_connections: int):
    server = RustWSEServer(
        "0.0.0.0",
        port,
        max_connections,
        jwt_secret=JWT_SECRET,
        jwt_issuer=JWT_ISSUER,
        jwt_audience=JWT_AUDIENCE,
    )
    server.start()
    print(f"[bench-server] Rust WSE listening on 0.0.0.0:{port}")
    print(f"[bench-server] Max connections: {max_connections}")

    token = generate_token()
    print(f"[bench-server] JWT token: {token}")
    print("[bench-server] Ready. Press Ctrl+C to stop.")

    stop = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    # Drain loop — process inbound events so connections don't time out
    while not stop.is_set():
        try:
            batch = server.drain_inbound()
            if batch:
                for _event in batch:
                    pass  # Just consume, don't process
            else:
                await asyncio.sleep(0.001)
        except Exception as e:
            print(f"[bench-server] drain error: {e}")
            await asyncio.sleep(0.1)

    server.stop()
    print("[bench-server] Stopped.")


def main():
    parser = argparse.ArgumentParser(description="Minimal WSE benchmark server")
    parser.add_argument("--port", type=int, default=5006)
    parser.add_argument("--max-connections", type=int, default=5000)
    parser.add_argument("--token-only", action="store_true", help="Just print a JWT token and exit")
    args = parser.parse_args()

    if args.token_only:
        print(generate_token())
        return

    asyncio.run(run_server(args.port, args.max_connections))


if __name__ == "__main__":
    main()
