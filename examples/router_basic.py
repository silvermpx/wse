"""WSE Router mode -- embedded in FastAPI.

Mount WSE as a FastAPI router. One process, one port.
No Rust compilation needed -- just pip install.

    pip install wse-server uvicorn
    python examples/router_basic.py

Then open http://localhost:8000/wse/health to check status.
WebSocket endpoint: ws://localhost:8000/wse
"""

import asyncio
import logging

import uvicorn
from fastapi import FastAPI, WebSocket

from wse_server import WSEConfig, create_wse_router

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="WSE Router Example")


# -- Auth handler (optional) --------------------------------------------------

async def auth_handler(websocket: WebSocket) -> str | None:
    """Extract user_id from JWT cookie or Authorization header.

    Return user_id string on success, None to reject.
    Remove this handler to allow anonymous connections.
    """
    import jwt

    secret = "my-secret-key"
    token = None

    # try cookie first (browsers)
    cookies = websocket.cookies
    if "access_token" in cookies:
        token = cookies["access_token"]

    # fallback to Authorization header (backend clients)
    if not token:
        auth = websocket.headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            token = auth[7:]

    if not token:
        return None

    try:
        payload = jwt.decode(token, secret, algorithms=["HS256"])
        return payload.get("sub")
    except jwt.PyJWTError:
        return None


# -- WSE setup ----------------------------------------------------------------

wse = create_wse_router(WSEConfig(
    auth_handler=auth_handler,
    default_topics=["notifications", "system"],
    heartbeat_interval=15.0,
    idle_timeout=90.0,
))

app.include_router(wse, prefix="/wse")


# -- Background publisher (simulate events) -----------------------------------

async def publish_loop():
    """Wait for PubSub bus to be ready, then publish events."""
    # wait for startup
    await asyncio.sleep(2)

    bus = getattr(app.state, "pubsub_bus", None)
    if not bus:
        print("[!] PubSubBus not available -- is Redis running?")
        return

    seq = 0
    while True:
        await bus.publish(
            topic="notifications",
            event={
                "event_type": "system_alert",
                "text": f"Server heartbeat #{seq}",
                "seq": seq,
            },
        )
        seq += 1
        await asyncio.sleep(5.0)


@app.on_event("startup")
async def startup():
    asyncio.create_task(publish_loop())


# -- REST endpoints alongside WebSocket ---------------------------------------

@app.get("/")
async def root():
    return {"service": "WSE Router Example", "ws_endpoint": "/wse"}


@app.post("/api/notify")
async def send_notification(message: str = "Hello!"):
    """Send a notification to all connected clients via REST."""
    bus = getattr(app.state, "pubsub_bus", None)
    if not bus:
        return {"error": "PubSubBus not available"}

    await bus.publish(
        topic="notifications",
        event={"event_type": "notification", "text": message},
    )
    return {"status": "sent", "topic": "notifications"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
