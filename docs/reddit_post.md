# r/Python Showcase Post

**Title:** I built WSE — Rust-accelerated WebSocket engine for Python (2M msg/s, E2E encrypted)

---

been doing real-time backends for a while — trading, encrypted messaging between services, that kind of stuff. websockets in python are painfully slow once you need actual throughput. tried the usual — pure python libs cap at maybe 10k msg/s, then you're looking at rewriting in go or running some separate server and gluing it together with redis.

so i built wse. it's a websocket engine that sits inside fastapi. you write python, the transport runs in rust (pyo3). tcp accept, framing, compression, encryption, ping/pong — all rust. python never sees raw frames.

**What My Project Does**

the fast part comes from two things:

jwt validation happens in rust during the ws handshake. cookie parsing, hs256 sig check, expiry — done before python knows someone connected. 0.5ms instead of 23ms. python just gets the user_id.

then there's drain mode. rust queues up incoming messages, python grabs them in batches. one gil acquire per batch, not per message. same on the way out — write coalescing, up to 64 messages flushed in one syscall.

```python
from wse_server import create_wse_router, WSEConfig

config = WSEConfig(
    jwt_secret="your-secret",
    compression=True,
    encryption=True,
)
app.include_router(create_wse_router(config))
```

2M msg/s on epyc, 0.5M+ on a macbook m2. e2e encryption (ecdh p-256 + aes-gcm-256), zlib, msgpack, pub/sub, dedup, circuit breaker, rate limiting.

has ts/react and python clients:

```python
from wse_client import connect

async with connect("ws://localhost:5006/wse", token="jwt...") as client:
    await client.subscribe(["notifications"])
    async for event in client:
        print(event.type, event.payload)
```

376+ tests across the whole thing — pytest for server and python client, vitest for ts client. ci runs on python 3.12-3.14, binary wheels on pypi.

**Target Audience**

if you have a python backend pushing real-time data and you're hitting the wall with pure python websockets but don't want to maintain a separate go/rust service — this is for that. i run it in production for trading (latency matters) and encrypted service-to-service messaging.

**Comparison**

most python ws libs are pure python so you're bottlenecked by the interpreter on framing and serialization. the typical solution is to stick a go/rust server in front and talk to it over ipc or redis — works but now you have two services, two deploys, serialization overhead in between. wse just runs rust inside your python process via pyo3. one process, one deploy, business logic stays in python.

https://github.com/silvermpx/wse
`pip install wse-server` / `pip install wse-client` / `npm install wse-client`

---

## First Comment

some context on the architecture: wse has two modes. "router mode" embeds into your fastapi app on the same port — zero config, good for prototyping. "standalone mode" runs a separate rust tokio server with no python on the hot path at all — that's where the 2M numbers come from.

the optimization journey was fun. biggest wins were orjson (3-5x faster json), moving from per-message callbacks to batch drain (3-4x), and putting jwt in rust (connection latency went from 23ms to 0.53ms). the last one was the most satisfying — just removing python from the handshake entirely.

happy to go deep on any of this.
